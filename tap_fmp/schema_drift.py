"""Compares the records FMP sends against a stream's declared schema."""

from __future__ import annotations

import datetime as dt
import decimal
import logging
import typing as t


class SchemaDriftMonitor:
    """Names the fields of one stream that FMP added, stopped sending, stopped
    filling or changed the type of.

    A new field or type logs when first seen, so a stream that dies mid-run still
    names it. `report` repeats everything at the end of the sync, because stream
    logs keep only their tail. A field absent from every record cannot be told
    from a field FMP only sends sometimes, so the report carries the record count.
    """

    TOKEN: t.ClassVar[str] = "*** SCHEMA_DRIFT ***"
    # matched on exact type, so a bool never passes as a number; dates are what a stream's own converters produce
    PYTHON_TYPES: t.ClassVar[dict[str, frozenset[type]]] = {
        "string": frozenset({str, dt.date, dt.datetime, dt.time}),
        "number": frozenset({int, float, decimal.Decimal}),
        "integer": frozenset({int}),
        "boolean": frozenset({bool}),
        "object": frozenset({dict}),
        "array": frozenset({list}),
    }

    def __init__(self, stream_name: str, schema: dict, logger: logging.Logger) -> None:
        self._stream_name = stream_name
        self._logger = logger
        self._declared_names = {
            field: self._type_names(spec)
            for field, spec in schema.get("properties", {}).items()
        }
        self._accepted = {
            field: self._python_types(names)
            for field, names in self._declared_names.items()
        }
        self._records = 0
        self._arrived: set[str] = set()
        self._filled: set[str] = set()
        self._new_fields: set[str] = set()
        self._changed_types: set[str] = set()

    @staticmethod
    def _type_names(spec: dict) -> list[str]:
        declared = spec.get("type", [])
        names = [declared] if isinstance(declared, str) else declared
        return [name for name in names if name != "null"]

    @classmethod
    def _python_types(cls, names: list[str]) -> frozenset[type]:
        return frozenset().union(*(cls.PYTHON_TYPES[name] for name in names))

    def observe(self, record: dict) -> None:
        self._records += 1
        self._arrived.update(record)
        for field, value in record.items():
            accepted = self._accepted.get(field)
            if accepted is None:
                self._flag_new_field(field)
            elif value is not None:
                self._filled.add(field)
                # a field declared without a JSON type (anyOf) accepts nothing here, so it goes unchecked
                if accepted and type(value) not in accepted:
                    self._flag_changed_type(field, type(value).__name__)

    def _flag_new_field(self, field: str) -> None:
        if field not in self._new_fields:
            self._new_fields.add(field)
            self._log(f"new_field={field} action=add_to_schema")

    def _flag_changed_type(self, field: str, observed: str) -> None:
        change = f"{field}:{'/'.join(self._declared_names[field])}->{observed}"
        if change not in self._changed_types:
            self._changed_types.add(change)
            self._log(f"changed_type={change} action=fix_schema_or_converter")

    def report(self) -> None:
        """Log the whole run's drift; a stream that returned nothing says nothing about its schema."""
        if not self._records:
            return
        absent = self._accepted.keys() - self._arrived
        always_null = (self._arrived & self._accepted.keys()) - self._filled
        if self._new_fields or self._changed_types or absent or always_null:
            self._log(
                f"records={self._records} new_fields={sorted(self._new_fields)} "
                f"changed_types={sorted(self._changed_types)} absent_fields={sorted(absent)} "
                f"always_null_fields={sorted(always_null)}"
            )

    def _log(self, detail: str) -> None:
        self._logger.error(f"{self.TOKEN} stream={self._stream_name} {detail}")
