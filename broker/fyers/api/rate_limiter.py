"""Shared rate limiting and 429-retry helpers for Fyers API calls.

Fyers Standard separates market-data quotas from transaction and user-info
quotas. OpenAlgo keeps headroom inside both published categories; the streaming
proxy has a reserved portion of the market-data budget because it runs in a
separate process from the Flask worker.
"""

import threading
import time
from collections import deque

from utils.broker_backpressure import BrokerBusyError, cap_server_delay, check_queue_wait

DATA_MAX_CALLS_PER_SECOND = 3
DATA_MAX_CALLS_PER_MINUTE = 40
DATA_MAX_CALLS_PER_DAY = 4_000

DATA_STREAM_MAX_CALLS_PER_SECOND = 1
DATA_STREAM_MAX_CALLS_PER_MINUTE = 5
DATA_STREAM_MAX_CALLS_PER_DAY = 500

ORDER_MAX_CALLS_PER_SECOND = 8
ORDER_MAX_CALLS_PER_MINUTE = 180
ORDER_MAX_CALLS_PER_DAY = 90_000

WINDOW_SECONDS = 60.0
DAY_WINDOW_SECONDS = 24 * 60 * 60.0
WINDOW_HEADROOM_SECONDS = 0.01
MAX_RETRIES = 3
BASE_BACKOFF = 1.0  # seconds; exponential fallback when no Retry-After header: 1, 2, 4

_LIMITS = {
    "data": {
        "per_second": DATA_MAX_CALLS_PER_SECOND,
        "per_minute": DATA_MAX_CALLS_PER_MINUTE,
        "per_day": DATA_MAX_CALLS_PER_DAY,
        "min_interval": 1 / DATA_MAX_CALLS_PER_SECOND,
    },
    "data_stream": {
        "per_second": DATA_STREAM_MAX_CALLS_PER_SECOND,
        "per_minute": DATA_STREAM_MAX_CALLS_PER_MINUTE,
        "per_day": DATA_STREAM_MAX_CALLS_PER_DAY,
        "min_interval": 1 / DATA_STREAM_MAX_CALLS_PER_SECOND,
    },
    "order": {
        "per_second": ORDER_MAX_CALLS_PER_SECOND,
        "per_minute": ORDER_MAX_CALLS_PER_MINUTE,
        "per_day": ORDER_MAX_CALLS_PER_DAY,
        "min_interval": 1 / ORDER_MAX_CALLS_PER_SECOND,
    },
}
_WINDOWS = {
    "second": (1.0, "per_second"),
    "minute": (WINDOW_SECONDS, "per_minute"),
    "day": (DAY_WINDOW_SECONDS, "per_day"),
}
_lock = threading.Lock()
_last_call_time = dict.fromkeys(_LIMITS, 0.0)
_cooldown_until = dict.fromkeys(_LIMITS, 0.0)
_reserved_call_times = {
    kind: {window: deque() for window in _WINDOWS} for kind in _LIMITS
}


def _normalize_kind(kind):
    if kind not in _LIMITS:
        raise ValueError(f"unknown Fyers API quota category {kind!r}")
    return kind


def _queue_kind(kind):
    return "order" if kind == "order" else "data"


def api_category_for_endpoint(endpoint):
    """Return the applicable Fyers quota category for a REST endpoint."""
    path = endpoint.partition("?")[0]
    return "data" if path.startswith("/data/") else "order"


def _daily_budget_message(kind):
    if kind == "order":
        return (
            "OpenAlgo has reached its daily Fyers transaction and account-data "
            "request budget. Wait for earlier requests to leave the rolling "
            "24-hour window or reduce other requests."
        )
    return (
        "OpenAlgo has reached its daily Fyers market-data request budget. "
        "Wait for earlier requests to leave the rolling 24-hour window or "
        "reduce other data refreshes."
    )


def _reserve_persistent_daily_slot(kind, limit, now):
    from database.fyers_usage_db import reserve_daily_request

    return reserve_daily_request(kind, limit, now, DAY_WINDOW_SECONDS)


def apply_rate_limit(kind="data"):
    """Reserve a request slot inside all published windows for its category.

    The regular data budget and transaction/user-info budget are independent.
    ``data_stream`` is a reserved share for Fyers' symbol-token and TBT setup
    requests, which originate in the separate WebSocket-proxy process.

    Under gthread, a caller whose slot is further away than the shared
    backpressure policy permits is refused before it books anything. Daily
    budget exhaustion is refused in every worker mode rather than sleeping
    until a future day.

    Args:
        kind: ``"data"``, ``"data_stream"``, or ``"order"``. ``"order"``
            covers Fyers transaction and user-info endpoints.

    Raises:
        BrokerBusyError: When gthread cannot wait for the slot, or the local
            daily budget for the category has been used.
    """
    kind = _normalize_kind(kind)
    limits = _LIMITS[kind]
    reservations = _reserved_call_times[kind]

    while True:
        with _lock:
            now = time.time()
            for window, (duration, _) in _WINDOWS.items():
                timestamps = reservations[window]
                while timestamps and timestamps[0] <= now - duration:
                    timestamps.popleft()

            day_timestamps = reservations["day"]
            if len(day_timestamps) >= limits["per_day"]:
                raise BrokerBusyError(_daily_budget_message(kind))

            next_call_time = max(
                now,
                _last_call_time[kind] + limits["min_interval"],
                _cooldown_until[kind],
            )
            for window, (duration, limit_name) in _WINDOWS.items():
                timestamps = reservations[window]
                if len(timestamps) >= limits[limit_name]:
                    next_call_time = max(
                        next_call_time,
                        timestamps[0] + duration + WINDOW_HEADROOM_SECONDS,
                    )

            check_queue_wait(next_call_time - now, _queue_kind(kind))
            _last_call_time[kind] = next_call_time
            for timestamps in reservations.values():
                timestamps.append(next_call_time)

        if next_call_time > now:
            time.sleep(next_call_time - now)

        with _lock:
            if next_call_time >= _cooldown_until[kind]:
                break
            for timestamps in reservations.values():
                if next_call_time in timestamps:
                    timestamps.remove(next_call_time)

    if kind in ("data", "data_stream") and not _reserve_persistent_daily_slot(
        kind, limits["per_day"], time.time()
    ):
        raise BrokerBusyError(_daily_budget_message(kind))


def retry_delay_from_headers(headers, attempt, kind="data"):
    """Compute how long to wait before retrying a 429."""
    kind = _normalize_kind(kind)
    delay = _requested_delay(headers, attempt)
    if cap_server_delay(delay, _queue_kind(kind)) is None:
        raise BrokerBusyError(
            "Fyers asked OpenAlgo to slow down and wait longer than a request "
            "can be held. This request was not retried. Try again in a minute.",
            retry_after=delay,
        )
    return delay


def defer_after_rate_limit(headers, attempt, kind="data"):
    """Apply a broker-requested cooldown to every caller in this quota category."""
    kind = _normalize_kind(kind)
    delay = retry_delay_from_headers(headers, attempt, kind)
    with _lock:
        _cooldown_until[kind] = max(_cooldown_until[kind], time.time() + delay)
    return delay


def _requested_delay(headers, attempt):
    retry_after_ms = headers.get("X-Retry-After-Ms") or headers.get("x-retry-after-ms")
    if retry_after_ms:
        try:
            return max(float(retry_after_ms) / 1000.0, 0.05)
        except ValueError:
            pass

    retry_after = headers.get("Retry-After") or headers.get("retry-after")
    if retry_after:
        try:
            return max(float(retry_after), 0.05)
        except ValueError:
            pass

    return BASE_BACKOFF * (2**attempt)
