"""Testes unitários para o classificador e sanitizador de envelopes CDC."""

from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal

from scripts.confluent_utils import to_json_safe
from scripts.verify_cdc import classify_cdc_record


def test_to_json_safe_preserves_decimal_datetime_and_bytes() -> None:
    payload = {
        "amount": Decimal("1234.50"),
        "ts": datetime(2026, 11, 13, 6, 0, 0, tzinfo=UTC),
        "raw": b"\x01\xab",
        "nested": [Decimal("0.99"), None],
    }
    converted = to_json_safe(payload)
    assert converted == {
        "amount": "1234.50",
        "ts": "2026-11-13T06:00:00+00:00",
        "raw": "0x01ab",
        "nested": ["0.99", None],
    }


def test_classify_cdc_record_covers_all_debezium_ops() -> None:
    assert classify_cdc_record({"customer_id": 201}, None) == "tombstone"
    assert classify_cdc_record({"customer_id": 201}, {"op": "c"}) == "create"
    assert classify_cdc_record({"customer_id": 201}, {"op": "u"}) == "update"
    assert classify_cdc_record({"customer_id": 201}, {"op": "d"}) == "delete"
    assert classify_cdc_record({"customer_id": 201}, {"op": "r"}) == "read"
