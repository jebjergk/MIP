"""Unit tests for broker execution ↔ LIVE_ORDERS classification (no DB)."""

from app.services.broker_execution_reconcile import classify_execution_against_orders


def _idx(orders: list[dict]) -> dict:
    from app.services.broker_execution_reconcile import _index_orders_by_broker_id

    return _index_orders_by_broker_id(orders)


def test_matched_full_fill():
    ex = {
        "EXEC_KEY": "k1",
        "SYMBOL": "AAPL",
        "OPEN_ORDER_ID": "12345",
        "PAYLOAD": {"shares": 10.0, "price": 100.0},
    }
    orders = [
        {
            "ORDER_ID": "o1",
            "ACTION_ID": "a1",
            "BROKER_ORDER_ID": "12345",
            "STATUS": "SUBMITTED",
            "QTY_ORDERED": 10.0,
            "QTY_FILLED": 0.0,
        }
    ]
    c = classify_execution_against_orders(ex, _idx(orders))
    assert c["category"] == "matched"
    assert c["proposed_status"] == "FILLED"
    assert c["order_id"] == "o1"


def test_already_synced():
    ex = {
        "EXEC_KEY": "k1",
        "SYMBOL": "AAPL",
        "OPEN_ORDER_ID": "12345",
        "PAYLOAD": {"shares": 10.0, "price": 100.0},
    }
    orders = [
        {
            "ORDER_ID": "o1",
            "ACTION_ID": "a1",
            "BROKER_ORDER_ID": "12345",
            "STATUS": "FILLED",
            "QTY_ORDERED": 10.0,
        }
    ]
    c = classify_execution_against_orders(ex, _idx(orders))
    assert c["category"] == "already_synced"


def test_ambiguous_two_orders():
    ex = {
        "EXEC_KEY": "k1",
        "SYMBOL": "AAPL",
        "OPEN_ORDER_ID": "12345",
        "PAYLOAD": {},
    }
    orders = [
        {"ORDER_ID": "o1", "ACTION_ID": "a1", "BROKER_ORDER_ID": "12345", "STATUS": "SUBMITTED", "QTY_ORDERED": 1.0},
        {"ORDER_ID": "o2", "ACTION_ID": "a2", "BROKER_ORDER_ID": "12345", "STATUS": "SUBMITTED", "QTY_ORDERED": 1.0},
    ]
    c = classify_execution_against_orders(ex, _idx(orders))
    assert c["category"] == "ambiguous"


def test_partial_fill_proposed():
    ex = {
        "EXEC_KEY": "k1",
        "SYMBOL": "AAPL",
        "OPEN_ORDER_ID": "99",
        "PAYLOAD": {"shares": 3.0, "price": 50.0},
    }
    orders = [
        {
            "ORDER_ID": "o1",
            "ACTION_ID": "a1",
            "BROKER_ORDER_ID": "99",
            "STATUS": "SUBMITTED",
            "QTY_ORDERED": 10.0,
        }
    ]
    c = classify_execution_against_orders(ex, _idx(orders))
    assert c["category"] == "matched"
    assert c["proposed_status"] == "PARTIAL_FILL"
    assert c["proposed_qty_filled"] == 3.0


def test_avg_price_snake_case_from_execution_payload():
    """IBKR snapshots often use avg_price (snake); closeout economics need proposed_avg_fill_price."""
    ex = {
        "EXEC_KEY": "k1",
        "SYMBOL": "SBUX",
        "OPEN_ORDER_ID": "686115300",
        "PAYLOAD": {"shares": "2", "avg_price": "94.91"},
    }
    orders = [
        {
            "ORDER_ID": "o1",
            "ACTION_ID": "a1",
            "BROKER_ORDER_ID": "686115300",
            "STATUS": "PENDINGSUBMIT",
            "QTY_ORDERED": 2.0,
            "QTY_FILLED": 0.0,
        }
    ]
    c = classify_execution_against_orders(ex, _idx(orders))
    assert c["category"] == "matched"
    assert c["proposed_status"] == "FILLED"
    assert c["proposed_avg_fill_price"] == 94.91


def test_build_missing_close_execution_placeholder():
    from app.services.broker_execution_reconcile import build_missing_close_execution_placeholders

    rows = build_missing_close_execution_placeholders(
        [{"symbol": "AAPL", "last_known_qty": 1.0, "last_position_ts": "2026-07-06T16:14:03"}]
    )
    assert len(rows) == 1
    assert rows[0]["symbol"] == "AAPL"
    assert rows[0]["missing_fill"] is True
    assert rows[0]["status"] == "MISSING_FILL"


def test_detect_missing_close_fill_gaps():
    from unittest.mock import MagicMock, patch

    from app.services.broker_execution_reconcile import detect_missing_close_fill_gaps

    cur = MagicMock()
    with patch("app.services.broker_execution_reconcile.fetch_all") as mock_fetch:
        mock_fetch.return_value = [
            {"SYMBOL": "AAPL", "MAX_ABS_QTY": 1.0, "LAST_POSITION_TS": "2026-07-06"},
            {"SYMBOL": "COP", "MAX_ABS_QTY": 4.0, "LAST_POSITION_TS": "2026-07-20"},
        ]
        gaps = detect_missing_close_fill_gaps(
            cur=cur,
            account_id="U24621464",
            lookback_days=30,
            held_symbols={"COP"},
            executions=[{"symbol": "NVDA", "side": "SELL", "close_like": True}],
        )
    symbols = {g["symbol"] for g in gaps}
    assert "AAPL" in symbols
    assert "COP" not in symbols
