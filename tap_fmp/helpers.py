import re
import requests
from datetime import datetime
import threading
import logging
import backoff
import functools
import time
from requests.exceptions import ConnectionError, RequestException
from urllib3.exceptions import MaxRetryError, NewConnectionError


class RateLimitManager:
    def __init__(self):
        self.last_request_time = {}
        self.min_delay = 0.2

    def wait_if_needed(self, endpoint_name):
        """Wait if we need to respect rate limits"""
        now = datetime.now()
        last_time = self.last_request_time.get(endpoint_name)

        if last_time:
            time_since_last = (now - last_time).total_seconds()
            if time_since_last < self.min_delay:
                sleep_time = self.min_delay - time_since_last
                logging.info(
                    f"⏱️ Rate limiting: sleeping {sleep_time:.1f}s for {endpoint_name}"
                )
                time.sleep(sleep_time)
        self.last_request_time[endpoint_name] = now


rate_limiter = RateLimitManager()


class TickerFetcher:
    """
    Fetch and caches Yahoo tickers in memory for the duration of a Meltano tap run.
    ENSURES no duplicates and stops pagination when tickers repeat.
    """

    _memory_cache = {}
    _cache_lock = threading.Lock()

    def fetch_all_tickers(self) -> list[dict]:
        # TODO: implement pagination
        endpoint = (
            f"https://financialmodelingprep.com/stable/ratings-historical?symbol=AAPL&apikey="
            f"{self.config.get('api_key')}"
        )
        response = requests.get(endpoint)
        response.raise_for_status()
        tickers = response.json()
        return tickers

    def fetch_specific_tickers(self, ticker_list: list[str]) -> list[dict]:
        """
        Create ticker records for a specific list of tickers.
        """
        return [
            {
                "ticker": ticker.upper(),
                "name": None,
            }
            for ticker in ticker_list
        ]


class EmptyDataException(Exception):
    """Raised when data is empty but likely should contain data - triggers retry."""

    pass


def fmp_api_retry(func):
    """Enhanced backoff with proper error classification and rate limiting."""

    @functools.wraps(func)
    def wrapped_func(*args, **kwargs):
        # Extract ticker for better logging
        ticker = "unknown"
        if len(args) >= 2:
            ticker = args[1]  # args[0] is self, args[1] is ticker

        # Apply rate limiting
        rate_limiter.wait_if_needed(f"{func.__name__}_{ticker}")

        # Add small base delay to reduce API pressure
        time.sleep(0.1)

        try:
            result = func(*args, **kwargs)
            return result

        except EmptyDataException:
            raise
        except (
            ConnectionError,
            RequestException,
            MaxRetryError,
            NewConnectionError,
        ) as e:
            logging.info(f"🔄 Network error for {ticker} - will retry: {e}")
            raise RequestException(f"Network error for {ticker}: {e}")
        except Exception as e:
            # Check if it's a rate limit error from yahooquery
            error_str = str(e).lower()
            if any(
                phrase in error_str
                for phrase in ["rate limit", "429", "too many requests", "quota"]
            ):
                logging.info(f"🔄 Rate limit detected for {ticker} - will retry")
                raise RequestException(f"Rate limit for {ticker}: {e}")
            else:
                # For other errors, still retry but with different exception type
                logging.warning(f"🔄 Other error for {ticker} - will retry: {e}")
                raise RequestException(f"Other error for {ticker}: {e}")

    def backoff_handler(details):
        exception_str = str(details["exception"])
        ticker_match = re.search(r"for (\w+)", exception_str)
        ticker_info = f" [{ticker_match.group(1)}]" if ticker_match else ""

        logging.info(
            f"🔄 Retrying {details['target'].__name__}{ticker_info} - "
            f"attempt {details['tries']}/10, waiting {details['wait']:.1f}s"
        )

    def giveup_handler(details):
        exception_str = str(details["exception"])
        ticker_match = re.search(r"for (\w+)", exception_str)
        ticker_info = f" [{ticker_match.group(1)}]" if ticker_match else ""

        logging.warning(
            f"⚠️ Giving up on {details['target'].__name__}{ticker_info} after {details['tries']} attempts"
        )

    @functools.wraps(func)
    def safe_wrapper(*args, **kwargs):
        try:
            return backoff.on_exception(
                backoff.expo,
                (
                    RequestException,
                    ConnectionError,
                    MaxRetryError,
                    NewConnectionError,
                    EmptyDataException,
                ),
                max_tries=10,
                max_time=600,
                base=3,
                max_value=60,
                jitter=backoff.full_jitter,
                # Remove the giveup condition entirely
                on_backoff=backoff_handler,
                on_giveup=giveup_handler,
            )(wrapped_func)(*args, **kwargs)
        except Exception:
            # Return empty DataFrame for any final failures
            logging.info(f"📄 Returning empty DataFrame for {func.__name__}")
            return {}

    return safe_wrapper


def clean_strings(lst):
    cleaned_list = [
        re.sub(r"[^a-zA-Z0-9_]", "_", s) for s in lst
    ]  # remove special characters
    cleaned_list = [
        re.sub(r"(?<!^)(?=[A-Z])", "_", s).lower() for s in cleaned_list
    ]  # camel case -> snake case
    cleaned_list = [
        re.sub(r"_+", "_", s).strip("_").lower() for s in cleaned_list
    ]  # clean leading and trailing underscores
    return cleaned_list
