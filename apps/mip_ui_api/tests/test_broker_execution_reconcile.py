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
