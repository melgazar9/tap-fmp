"""Fetches the partitions after the one being written, on worker threads."""

from __future__ import annotations

import logging
import threading
import typing as t
from collections import deque
from dataclasses import dataclass


@dataclass(frozen=True)
class PartitionPlan:
    """What a worker needs to fetch one partition, worked out on the main thread, which owns Singer state."""

    context: dict
    start: str | None
    query_params: dict
    windows: list[tuple[str, str]]


class PartitionPrefetchStopped(RuntimeError):
    """A worker dropped its partition: an earlier partition failed, or the writer stopped reading."""


class _PartitionPages:
    """One queued partition's raw pages, on their way from its worker to the writer."""

    def __init__(self, position: int, plan: PartitionPlan):
        self.position = position
        self.plan = plan
        self.batches: deque[list[dict]] = deque()
        self.complete = False
        self.error: BaseException | None = None


class PartitionPrefetcher:
    """Fetches up to `max_queued_partitions` partitions ahead of the writer on `workers` threads.

    The writer takes each partition's pages in partition order on the main thread, so records,
    bookmarks and state messages keep the SDK's sequential order and workers never touch state.
    Rows held for partitions the writer has not reached stay under `max_buffered_records`; the
    partition being written may pass it, so the writer never waits on rows another partition holds.
    A failed partition stops the partitions after it; the writer still gets every partition before
    it, then the failure, which is what a one-at-a-time run writes before it stops.
    """

    def __init__(
        self,
        contexts: list[dict],
        plan: t.Callable[[dict], PartitionPlan],
        fetch: t.Callable[[PartitionPlan], t.Iterator[list[dict]]],
        *,
        workers: int,
        max_buffered_records: int,
        max_queued_partitions: int,
        name: str,
        logger: logging.Logger,
    ):
        if workers < 1 or max_queued_partitions < 1:
            raise ValueError(
                f"{name}: prefetching needs at least one worker and one queued partition, "
                f"got workers={workers} max_queued_partitions={max_queued_partitions}"
            )
        self._contexts = contexts
        self._plan = plan
        self._fetch = fetch
        self._max_buffered_records = max_buffered_records
        self._max_queued_partitions = max_queued_partitions
        self._name = name
        self._cond = threading.Condition()
        self._queued: deque[_PartitionPages] = deque()
        self._unstarted: deque[_PartitionPages] = deque()
        self._next_position = 0
        self._buffered_records = 0
        self._first_failed_position: int | None = None
        self._closed = False
        for i in range(workers):
            threading.Thread(
                target=self._work, name=f"{name}-fetch-{i}", daemon=True
            ).start()
        logger.info(
            f"Stream {name}: fetching up to {max_queued_partitions} partitions ahead on {workers} threads, "
            f"holding at most {max_buffered_records} rows for partitions not yet written."
        )

    @property
    def closed(self) -> bool:
        return self._closed

    def expects(self, context: dict) -> bool:
        """Whether `context` is the partition the writer reads next."""
        if self._closed:
            return False
        if self._queued:
            return self._queued[0].plan.context == context
        return (
            self._next_position < len(self._contexts)
            and self._contexts[self._next_position] == context
        )

    def batches(self, context: dict, start: str | None) -> t.Iterator[list[dict]]:
        """The next partition's raw pages in fetch order; it must be `context`, queued from `start`."""
        self._queue_ahead()
        head = self._queued[0]
        if head.plan.context != context or head.plan.start != start:
            raise RuntimeError(
                f"Stream {self._name}: the writer asked for {context} from {start}, but the next "
                f"fetched partition is {head.plan.context} from {head.plan.start}."
            )
        while (batch := self._take(head)) is not None:
            yield batch
        self._finish_head()

    def close(self) -> None:
        """Stops every worker; a worker mid-request drops what it fetches."""
        with self._cond:
            self._closed = True
            self._cond.notify_all()

    def _queue_ahead(self) -> None:
        """Plans the next partitions on the calling thread, the one that owns state."""
        while (
            self._next_position < len(self._contexts)
            and len(self._queued) < self._max_queued_partitions
        ):
            context = self._contexts[self._next_position]
            pages = _PartitionPages(self._next_position, self._plan(context))
            self._next_position += 1
            with self._cond:
                self._queued.append(pages)
                self._unstarted.append(pages)
                self._cond.notify_all()

    def _take(self, head: _PartitionPages) -> list[dict] | None:
        """The head's next batch, None once it is complete; its failure once its batches are taken."""
        with self._cond:
            while True:
                if head.batches:
                    batch = head.batches.popleft()
                    self._buffered_records -= len(batch)
                    self._cond.notify_all()
                    return batch
                if head.complete:
                    return None
                if head.error is not None:
                    raise head.error
                if self._closed:
                    raise PartitionPrefetchStopped(
                        f"Stream {self._name}: closed before {head.plan.context} was fetched"
                    )
                self._cond.wait()

    def _finish_head(self) -> None:
        with self._cond:
            self._queued.popleft()
            # a worker waiting for room may now hold the head
            self._cond.notify_all()
        if not self._queued and self._next_position >= len(self._contexts):
            self.close()

    def _work(self) -> None:
        while True:
            with self._cond:
                while not self._unstarted and not self._closed:
                    self._cond.wait()
                if self._closed:
                    return
                pages = self._unstarted.popleft()
            if not self._dropped(pages):
                self._fill(pages)

    def _dropped(self, pages: _PartitionPages) -> bool:
        """Never written: the writer stopped reading, or stops at an earlier partition that failed."""
        return self._closed or (
            self._first_failed_position is not None
            and pages.position > self._first_failed_position
        )

    def _fill(self, pages: _PartitionPages) -> None:
        try:
            for batch in self._fetch(pages.plan):
                self._hand_over(pages, batch)
        except PartitionPrefetchStopped:
            return
        except Exception as exc:
            with self._cond:
                pages.error = exc
                if (
                    self._first_failed_position is None
                    or pages.position < self._first_failed_position
                ):
                    self._first_failed_position = pages.position
                self._cond.notify_all()
            return
        with self._cond:
            pages.complete = True
            self._cond.notify_all()

    def _hand_over(self, pages: _PartitionPages, batch: list[dict]) -> None:
        with self._cond:
            while not self._dropped(pages) and not self._has_room(pages, len(batch)):
                self._cond.wait()
            if self._dropped(pages):
                raise PartitionPrefetchStopped(
                    f"Stream {self._name}: stopped fetching {pages.plan.context}"
                )
            pages.batches.append(batch)
            self._buffered_records += len(batch)
            self._cond.notify_all()

    def _has_room(self, pages: _PartitionPages, rows: int) -> bool:
        """The writer's own partition may always hand over one batch, so others' rows never starve it."""
        is_head = bool(self._queued) and self._queued[0] is pages
        return self._buffered_records + rows <= self._max_buffered_records or (
            is_head and not pages.batches
        )
