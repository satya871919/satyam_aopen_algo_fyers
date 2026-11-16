from threading import Lock
from types import SimpleNamespace


def _adapter(subscriptions=None, tbt_subscriptions=None):
    from broker.fyers.streaming.fyers_websocket_adapter import FyersWebSocketAdapter

    adapter = FyersWebSocketAdapter.__new__(FyersWebSocketAdapter)
    adapter.connected = True
    adapter.fyers_adapter = SimpleNamespace(connected=True)
    adapter.lock = Lock()
    adapter.subscriptions = subscriptions or {}
    adapter.tbt_subscriptions = tbt_subscriptions or {}
    adapter.logger = SimpleNamespace(
        debug=lambda *_args: None,
        info=lambda *_args: None,
        warning=lambda *_args: None,
        error=lambda *_args: None,
    )
    return adapter


def test_fyers_data_socket_rejects_the_101st_unique_symbol():
    subscriptions = {
        f"NSE:SYM{i}:2": {"symbol": f"SYM{i}", "exchange": "NSE", "mode": 2}
        for i in range(100)
    }
    adapter = _adapter(subscriptions)
    queued = []
    adapter._enqueue_hsm_subscribe = lambda *args: queued.append(args)

    result = adapter.subscribe("NEW", "NSE", mode=2)

    assert result["status"] == "error"
    assert "100 symbols" in result["message"]
    assert not queued
    assert len(adapter.subscriptions) == 100


def test_fyers_data_socket_counts_one_symbol_once_across_modes():
    subscriptions = {
        f"NSE:SYM{i}:2": {"symbol": f"SYM{i}", "exchange": "NSE", "mode": 2}
        for i in range(100)
    }
    adapter = _adapter(subscriptions)
    queued = []
    adapter._enqueue_hsm_subscribe = lambda *args: queued.append(args)

    result = adapter.subscribe("SYM0", "NSE", mode=1)

    assert result["status"] == "success"
    assert len(queued) == 1
    assert len(adapter.subscriptions) == 101


def test_fyers_tbt_socket_rejects_a_second_symbol():
    subscriptions = {
        "NSE:SBIN:50:3": {"symbol": "SBIN:50", "exchange": "NSE", "mode": 3}
    }
    tbt_subscriptions = {
        "NSE:SBIN:50": {
            "actual_symbol": "SBIN",
            "exchange": "NSE",
        }
    }
    adapter = _adapter(subscriptions, tbt_subscriptions)
    tbt_attempts = []
    adapter._subscribe_tbt_depth = lambda *args: tbt_attempts.append(args) or False
    adapter._enqueue_hsm_subscribe = lambda *_args: None

    result = adapter.subscribe("TCS:50", "NSE", mode=3)

    assert result["status"] == "error"
    assert "one symbol" in result["message"]
    assert len(adapter.subscriptions) == 1
    assert not tbt_attempts


def test_fyers_connection_pool_uses_the_broker_socket_symbol_cap(monkeypatch):
    from broker.fyers.streaming.fyers_websocket_adapter import FyersWebSocketAdapter
    from websocket_proxy import broker_factory

    monkeypatch.setitem(broker_factory.BROKER_ADAPTERS, "fyers", FyersWebSocketAdapter)

    adapter = broker_factory.create_broker_adapter("fyers", use_pooling=True)

    assert adapter._max_symbols == 100


def test_fyers_tbt_url_setup_retries_rate_limited_data_request(monkeypatch):
    from broker.fyers.streaming import fyers_tbt_websocket

    responses = iter(
        [
            SimpleNamespace(
                status_code=429,
                headers={"Retry-After": "1"},
                raise_for_status=lambda: None,
            ),
            SimpleNamespace(
                status_code=200,
                headers={},
                json=lambda: {"data": {"socket_url": "wss://fyers.example/tbt"}},
                raise_for_status=lambda: None,
            ),
        ]
    )
    delays = []
    calls = []
    client = SimpleNamespace(
        get=lambda *_args, **_kwargs: (calls.append(True) or next(responses))
    )
    monkeypatch.setattr(fyers_tbt_websocket, "get_httpx_client", lambda: client)
    monkeypatch.setattr(
        fyers_tbt_websocket,
        "apply_rate_limit",
        lambda kind: calls.append(kind),
    )
    monkeypatch.setattr(
        fyers_tbt_websocket,
        "defer_after_rate_limit",
        lambda headers, attempt, **kwargs: (
            delays.append((headers, attempt, kwargs)) or 1.0
        ),
    )
    monkeypatch.setattr(fyers_tbt_websocket.time, "sleep", lambda _seconds: None)
    adapter = fyers_tbt_websocket.FyersTbtWebSocket.__new__(
        fyers_tbt_websocket.FyersTbtWebSocket
    )
    adapter.access_token = "token"
    adapter.logger = SimpleNamespace(debug=lambda *_args: None, warning=lambda *_args: None)

    assert adapter._get_tbt_url() == "wss://fyers.example/tbt"
    assert calls.count("data_stream") == 2
    assert delays == [
        ({"Retry-After": "1"}, 0, {"kind": "data_stream"})
    ]
