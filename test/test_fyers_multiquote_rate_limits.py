from collections import deque
from threading import Lock
from types import SimpleNamespace

import pytest

from utils.broker_backpressure import BrokerBusyError


def _reset_fyers_limiter(monkeypatch, rate_limiter, clock):
    monkeypatch.setattr(rate_limiter, "_lock", Lock())
    monkeypatch.setattr(
        rate_limiter,
        "_last_call_time",
        dict.fromkeys(rate_limiter._LIMITS, clock["now"] - 1),
    )
    monkeypatch.setattr(
        rate_limiter,
        "_cooldown_until",
        dict.fromkeys(rate_limiter._LIMITS, 0.0),
    )
    monkeypatch.setattr(
        rate_limiter,
        "_reserved_call_times",
        {
            kind: {window: deque() for window in rate_limiter._WINDOWS}
            for kind in rate_limiter._LIMITS
        },
    )
    monkeypatch.setattr(rate_limiter, "_reserve_persistent_daily_slot", lambda *_args: True)


def test_large_multiquote_skips_per_symbol_depth_requests(monkeypatch):
    from broker.fyers.api import data

    broker = data.BrokerData("token")
    batches = []
    monkeypatch.setattr(data.time, "sleep", lambda _seconds: None)
    monkeypatch.setattr(
        broker,
        "_process_quotes_batch",
        lambda symbols, fetch_oi: batches.append((len(symbols), fetch_oi)) or [],
    )

    broker.get_multiquotes([{"symbol": f"OPT{i}", "exchange": "NFO"} for i in range(51)])

    assert batches == [(50, False), (1, False)]


def test_small_multiquote_keeps_open_interest_fetch(monkeypatch):
    from broker.fyers.api import data

    broker = data.BrokerData("token")
    batches = []
    monkeypatch.setattr(
        broker,
        "_process_quotes_batch",
        lambda symbols, fetch_oi: batches.append((len(symbols), fetch_oi)) or [],
    )

    broker.get_multiquotes([{"symbol": f"OPT{i}", "exchange": "NFO"} for i in range(10)])

    assert batches == [(10, True)]


def test_fyers_data_pacer_respects_per_second_and_minute_caps(monkeypatch):
    from broker.fyers.api import rate_limiter

    clock = {"now": 1_000.0}
    fake_time = SimpleNamespace(
        time=lambda: clock["now"],
        sleep=lambda seconds: clock.__setitem__("now", clock["now"] + seconds),
    )
    monkeypatch.setattr(rate_limiter, "time", fake_time)
    _reset_fyers_limiter(monkeypatch, rate_limiter, clock)

    call_times = []
    for _ in range(rate_limiter.DATA_MAX_CALLS_PER_MINUTE * 2):
        rate_limiter.apply_rate_limit()
        call_times.append(rate_limiter._last_call_time["data"])

    for end, call_time in enumerate(call_times):
        for duration, limit in (
            (1, rate_limiter.DATA_MAX_CALLS_PER_SECOND),
            (rate_limiter.WINDOW_SECONDS, rate_limiter.DATA_MAX_CALLS_PER_MINUTE),
        ):
            in_window = sum(
                call_time - duration < t <= call_time for t in call_times[: end + 1]
            )
            assert in_window <= limit


def test_fyers_transaction_pacer_respects_its_independent_windows(monkeypatch):
    from broker.fyers.api import rate_limiter

    clock = {"now": 1_000.0}
    fake_time = SimpleNamespace(
        time=lambda: clock["now"],
        sleep=lambda seconds: clock.__setitem__("now", clock["now"] + seconds),
    )
    monkeypatch.setattr(rate_limiter, "time", fake_time)
    _reset_fyers_limiter(monkeypatch, rate_limiter, clock)

    call_times = []
    for _ in range(rate_limiter.ORDER_MAX_CALLS_PER_MINUTE * 2):
        rate_limiter.apply_rate_limit("order")
        call_times.append(rate_limiter._last_call_time["order"])

    for end, call_time in enumerate(call_times):
        for duration, limit in (
            (1, rate_limiter.ORDER_MAX_CALLS_PER_SECOND),
            (rate_limiter.WINDOW_SECONDS, rate_limiter.ORDER_MAX_CALLS_PER_MINUTE),
        ):
            in_window = sum(
                call_time - duration < t <= call_time for t in call_times[: end + 1]
            )
            assert in_window <= limit


def test_fyers_local_budgets_stay_inside_standard_plan_limits():
    from broker.fyers.api import data, rate_limiter

    assert (
        rate_limiter.DATA_MAX_CALLS_PER_SECOND
        + rate_limiter.DATA_STREAM_MAX_CALLS_PER_SECOND
        < 5
    )
    assert (
        rate_limiter.DATA_MAX_CALLS_PER_MINUTE
        + rate_limiter.DATA_STREAM_MAX_CALLS_PER_MINUTE
        <= 50
    )
    assert (
        rate_limiter.DATA_MAX_CALLS_PER_DAY
        + rate_limiter.DATA_STREAM_MAX_CALLS_PER_DAY
        <= 5_000
    )
    assert rate_limiter.ORDER_MAX_CALLS_PER_SECOND <= 10
    assert rate_limiter.ORDER_MAX_CALLS_PER_MINUTE <= 200
    assert rate_limiter.ORDER_MAX_CALLS_PER_DAY <= 100_000
    assert data.MULTIQUOTE_BATCH_SIZE == 50


def test_fyers_daily_data_budget_refuses_calls_without_waiting(monkeypatch):
    from broker.fyers.api import rate_limiter

    clock = {"now": 1_000.0}
    monkeypatch.setattr(
        rate_limiter,
        "time",
        SimpleNamespace(
            time=lambda: clock["now"],
            sleep=lambda _seconds: pytest.fail("daily exhaustion must not sleep"),
        ),
    )
    _reset_fyers_limiter(monkeypatch, rate_limiter, clock)
    rate_limiter._reserved_call_times["data"]["day"].extend(
        [clock["now"] - 1] * rate_limiter.DATA_MAX_CALLS_PER_DAY
    )

    with pytest.raises(BrokerBusyError, match="daily Fyers market-data"):
        rate_limiter.apply_rate_limit("data")

    rate_limiter.apply_rate_limit("order")


def test_fyers_data_request_is_refused_when_persistent_budget_is_exhausted(monkeypatch):
    from broker.fyers.api import rate_limiter

    clock = {"now": 1_000.0}
    fake_time = SimpleNamespace(
        time=lambda: clock["now"],
        sleep=lambda seconds: clock.__setitem__("now", clock["now"] + seconds),
    )
    monkeypatch.setattr(rate_limiter, "time", fake_time)
    _reset_fyers_limiter(monkeypatch, rate_limiter, clock)
    persistent_calls = []
    monkeypatch.setattr(
        rate_limiter,
        "_reserve_persistent_daily_slot",
        lambda kind, limit, now: persistent_calls.append((kind, limit)) or False,
    )

    with pytest.raises(BrokerBusyError, match="daily Fyers market-data"):
        rate_limiter.apply_rate_limit("data")

    assert persistent_calls == [("data", rate_limiter.DATA_MAX_CALLS_PER_DAY)]


def test_fyers_daily_budget_rolls_after_a_full_24_hour_window(monkeypatch):
    from broker.fyers.api import rate_limiter

    clock = {"now": 100_000.0}
    fake_time = SimpleNamespace(
        time=lambda: clock["now"],
        sleep=lambda seconds: clock.__setitem__("now", clock["now"] + seconds),
    )
    monkeypatch.setattr(rate_limiter, "time", fake_time)
    _reset_fyers_limiter(monkeypatch, rate_limiter, clock)
    rate_limiter._reserved_call_times["data"]["day"].extend(
        [clock["now"] - rate_limiter.DAY_WINDOW_SECONDS + 0.5]
        * rate_limiter.DATA_MAX_CALLS_PER_DAY
    )

    with pytest.raises(BrokerBusyError):
        rate_limiter.apply_rate_limit("data")

    clock["now"] += 0.6
    rate_limiter.apply_rate_limit("data")

    assert len(rate_limiter._reserved_call_times["data"]["day"]) == 1


def test_fyers_daily_budget_is_shared_across_database_engines(tmp_path, monkeypatch):
    from database import fyers_usage_db
    from database.engine_factory import create_db_engine

    database_path = tmp_path / "fyers-usage.db"
    first_engine = create_db_engine(f"sqlite:///{database_path.as_posix()}")
    monkeypatch.setattr(fyers_usage_db, "engine", first_engine)
    fyers_usage_db.init_db()

    assert fyers_usage_db.reserve_daily_request("data", 2, 1_000.0, 86_400)
    assert fyers_usage_db.reserve_daily_request("data", 2, 1_001.0, 86_400)

    second_engine = create_db_engine(f"sqlite:///{database_path.as_posix()}")
    monkeypatch.setattr(fyers_usage_db, "engine", second_engine)

    assert not fyers_usage_db.reserve_daily_request("data", 2, 1_002.0, 86_400)
    assert fyers_usage_db.reserve_daily_request("data_stream", 1, 1_002.0, 86_400)

    first_engine.dispose()
    second_engine.dispose()


def test_fyers_daily_budget_reservations_are_atomic_under_concurrency(
    tmp_path, monkeypatch
):
    from concurrent.futures import ThreadPoolExecutor
    from threading import Barrier

    from database import fyers_usage_db
    from database.engine_factory import create_db_engine

    database_path = tmp_path / "fyers-usage-concurrent.db"
    test_engine = create_db_engine(f"sqlite:///{database_path.as_posix()}")
    monkeypatch.setattr(fyers_usage_db, "engine", test_engine)
    fyers_usage_db.init_db()
    start = Barrier(12)

    def reserve_slot(index):
        start.wait()
        return fyers_usage_db.reserve_daily_request("data", 5, 2_000.0 + index, 86_400)

    with ThreadPoolExecutor(max_workers=12) as executor:
        results = list(executor.map(reserve_slot, range(12)))

    assert sum(results) == 5
    test_engine.dispose()


def test_fyers_daily_transaction_budget_refuses_calls_without_blocking(monkeypatch):
    from broker.fyers.api import rate_limiter

    clock = {"now": 1_000.0}
    monkeypatch.setattr(
        rate_limiter,
        "time",
        SimpleNamespace(
            time=lambda: clock["now"],
            sleep=lambda _seconds: pytest.fail("daily exhaustion must not sleep"),
        ),
    )
    _reset_fyers_limiter(monkeypatch, rate_limiter, clock)
    rate_limiter._reserved_call_times["order"]["day"].extend(
        [clock["now"] - 1] * rate_limiter.ORDER_MAX_CALLS_PER_DAY
    )

    with pytest.raises(BrokerBusyError, match="daily Fyers transaction"):
        rate_limiter.apply_rate_limit("order")

    rate_limiter.apply_rate_limit("data")


def test_fyers_429_cooldown_is_scoped_to_its_quota_category(monkeypatch):
    from broker.fyers.api import rate_limiter

    clock = {"now": 1_000.0}
    fake_time = SimpleNamespace(
        time=lambda: clock["now"],
        sleep=lambda seconds: clock.__setitem__("now", clock["now"] + seconds),
    )
    monkeypatch.setattr(rate_limiter, "time", fake_time)
    _reset_fyers_limiter(monkeypatch, rate_limiter, clock)

    assert rate_limiter.defer_after_rate_limit({"Retry-After": "2"}, 0, kind="data") == 2.0
    rate_limiter.apply_rate_limit("order")
    assert clock["now"] == 1_000.0
    rate_limiter.apply_rate_limit("data")
    assert clock["now"] >= 1_002.0


def test_fyers_history_429_registers_shared_cooldown(monkeypatch):
    import httpx

    from broker.fyers.api import data

    request = httpx.Request("GET", "https://api-t1.fyers.in/data/history")
    responses = iter(
        [
            httpx.Response(429, headers={"Retry-After": "1"}, request=request),
            httpx.Response(200, json={"s": "ok"}, request=request),
        ]
    )
    delays = []
    monkeypatch.setattr(
        data,
        "get_httpx_client",
        lambda: SimpleNamespace(get=lambda *_args, **_kwargs: next(responses)),
    )
    monkeypatch.setattr(data, "apply_rate_limit", lambda *_args: None)
    monkeypatch.setattr(
        data,
        "defer_after_rate_limit",
        lambda headers, attempt, **kwargs: delays.append((headers, attempt, kwargs)) or 1.0,
    )
    monkeypatch.setattr(data, "time", SimpleNamespace(sleep=lambda _seconds: None))

    result = data.get_api_response("/data/history", "token")

    assert result == {"s": "ok"}
    assert len(delays) == 1
    assert delays[0][0]["Retry-After"] == "1"
    assert delays[0][1] == 0
    assert delays[0][2]["kind"] == "data"


def test_fyers_endpoint_category_splits_data_from_account_calls():
    from broker.fyers.api.rate_limiter import api_category_for_endpoint

    assert api_category_for_endpoint("/data/history?symbol=NSE:SBIN") == "data"
    assert api_category_for_endpoint("/api/v3/positions") == "order"
    assert api_category_for_endpoint("/api/v3/funds") == "order"


def test_direct_order_calls_propagate_pacer_refusals(monkeypatch):
    from broker.fyers.api import order_api

    busy = BrokerBusyError("Fyers request queue is busy.")

    def refuse(*_args):
        raise busy

    monkeypatch.setattr(order_api, "get_httpx_client", lambda: object())
    monkeypatch.setattr(order_api, "transform_data", lambda _data: {})
    monkeypatch.setattr(order_api, "transform_modify_order_data", lambda _data: {})
    monkeypatch.setattr(order_api, "apply_rate_limit", refuse)

    calls = [
        (order_api.place_order_api, ({}, "token")),
        (order_api.close_all_positions, ("key", "token")),
        (order_api.cancel_order, ("order-id", "token")),
        (order_api.modify_order, ({}, "token")),
    ]
    for call, args in calls:
        with pytest.raises(BrokerBusyError) as raised:
            call(*args)
        assert raised.value is busy


def test_funds_and_margin_calls_propagate_pacer_refusals(monkeypatch):
    from broker.fyers.api import funds, margin_api

    busy = BrokerBusyError("Fyers request queue is busy.")
    monkeypatch.setenv("BROKER_API_KEY", "key")
    monkeypatch.setattr(funds, "get_httpx_client", lambda: object())
    monkeypatch.setattr(funds, "_cache", {})
    monkeypatch.setattr(funds, "_rate_limit", {})

    def refuse(*_args):
        raise busy

    monkeypatch.setattr(funds, "apply_rate_limit", refuse)

    with pytest.raises(BrokerBusyError) as raised:
        funds.get_margin_data("token")
    assert raised.value is busy

    monkeypatch.setattr(margin_api, "get_httpx_client", lambda: object())
    monkeypatch.setattr(margin_api, "transform_margin_positions", lambda _positions: ["valid"])
    monkeypatch.setattr(margin_api, "apply_rate_limit", refuse)

    with pytest.raises(BrokerBusyError) as raised:
        margin_api.calculate_margin_api([{}], "token")
    assert raised.value is busy
