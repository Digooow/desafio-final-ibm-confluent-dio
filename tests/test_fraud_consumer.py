"""Testes unitários, de fronteira e resiliência para o consumidor idempotente de fraude (Camada 4/5)."""

from __future__ import annotations

import sqlite3
from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path
from typing import Any
from unittest.mock import patch

import pytest
from confluent_kafka import KafkaError

from consumers.fraud_consumer import (
    FraudAlertRepository,
    consume_fraud_alerts,
    decode_raw_key,
    validate_alert_payload,
)
from consumers.fraud_consumer import (
    main as consumer_main,
)
from scripts.trigger_fraud_burst import (
    BURST_AMOUNTS,
    build_burst_timestamps,
    verify_idempotent_replay,
)


def _sample_alert(
    alert_id: str = "RULE_VELOCITY:42:2001",
    total_amount: Decimal = Decimal("1147.45"),
) -> dict[str, Any]:
    return {
        "alert_id": alert_id,
        "card_id": 42,
        "account_id": 15,
        "rule_name": "RULE_VELOCITY",
        "tx_count": 3,
        "total_amount": total_amount,
        "window_start": datetime(2026, 11, 13, 7, 10, 0, tzinfo=UTC),
        "window_end": datetime(2026, 11, 13, 7, 10, 12, tzinfo=UTC),
    }


def _dummy_env() -> dict[str, str]:
    return {
        "KAFKA_BOOTSTRAP_SERVERS": "pkc-example.us-east-1.aws.confluent.cloud:9092",
        "CONSUMER_API_KEY": "key",
        "CONSUMER_API_SECRET": "secret",
        "SCHEMA_REGISTRY_URL": "https://psrc-example.us-east-1.aws.confluent.cloud",
        "SCHEMA_REGISTRY_API_KEY": "sr_key",
        "SCHEMA_REGISTRY_API_SECRET": "sr_secret",
    }


class _FakeMessage:
    def __init__(self, raw_value: bytes | None, offset: int = 0, raw_key: bytes | None = b"k") -> None:
        self._value = raw_value
        self._offset = offset
        self._key = raw_key

    def error(self) -> Any:
        return None

    def topic(self) -> str:
        return "desafio.fraud.detected"

    def partition(self) -> int:
        return 0

    def offset(self) -> int:
        return self._offset

    def value(self) -> bytes | None:
        return self._value

    def key(self) -> bytes | None:
        return self._key


class _FakeConsumer:
    def __init__(self, messages: list[_FakeMessage]) -> None:
        self._messages = list(messages)
        self._assigned: list[Any] = []
        self.committed_offsets: list[int] = []
        self.closed = False

    def subscribe(self, _topics: list[str], on_assign: Any = None) -> None:
        from confluent_kafka import TopicPartition

        parts = [TopicPartition("desafio.fraud.detected", 0)]
        if on_assign is not None:
            on_assign(self, parts)
        else:
            self._assigned = parts

    def assign(self, partitions: list[Any]) -> None:
        self._assigned = partitions

    def assignment(self) -> list[Any]:
        return self._assigned

    def poll(self, _timeout: float) -> _FakeMessage | None:
        if self._messages:
            return self._messages.pop(0)
        return None

    def commit(self, message: _FakeMessage, asynchronous: bool = False) -> None:
        assert asynchronous is False
        self.committed_offsets.append(message.offset())

    def close(self) -> None:
        self.closed = True


def test_repository_applies_once_and_deduplicates_replay(tmp_path: Path) -> None:
    db_file = tmp_path / "alerts.db"
    repo = FraudAlertRepository(db_file)
    try:
        alert = _sample_alert()
        first = repo.apply_alert_idempotently(alert, "desafio.fraud.detected", 0, 10)
        second = repo.apply_alert_idempotently(alert, "desafio.fraud.detected", 0, 10)
        third = repo.apply_alert_idempotently(
            _sample_alert(alert_id="RULE_VELOCITY:99:2005"),
            "desafio.fraud.detected",
            0,
            11,
        )

        assert first == "applied"
        assert second == "duplicate"
        assert third == "applied"
        assert repo.summary_counts() == {"processed_alerts": 2, "fraud_actions": 2}
    finally:
        repo.close()


def test_validate_alert_payload_rejects_non_dict_with_type_error() -> None:
    with pytest.raises(TypeError):
        validate_alert_payload(None)


@pytest.mark.parametrize(
    "bad_payload",
    [
        {},
        {**_sample_alert(), "alert_id": "   "},
        {**_sample_alert(), "card_id": 0},
        {**_sample_alert(), "account_id": -1},
        {**_sample_alert(), "tx_count": 0},
        {**_sample_alert(), "total_amount": Decimal("0.00")},
        {**_sample_alert(), "total_amount": Decimal("-5.00")},
        {**_sample_alert(), "total_amount": "not-a-number"},
    ],
)
def test_validate_alert_payload_rejects_boundary_violations(bad_payload: dict[str, Any]) -> None:
    with pytest.raises(ValueError):
        validate_alert_payload(bad_payload)


def test_decode_raw_key_handles_none_and_utf8() -> None:
    assert decode_raw_key(None) is None
    assert decode_raw_key(b"RULE_VELOCITY:42:2001") == "RULE_VELOCITY:42:2001"


def test_consume_skips_poison_pill_and_closes_cleanly(tmp_path: Path) -> None:
    fake_consumer = _FakeConsumer([_FakeMessage(b"bad-bytes", offset=0), _FakeMessage(b"ok", offset=1)])
    db_file = tmp_path / "poison.db"

    def _fake_deser(raw: bytes, _ctx: Any) -> dict[str, Any]:
        if raw == b"bad-bytes":
            return {"alert_id": "bad", "card_id": -1}
        return _sample_alert()

    with (
        patch("consumers.fraud_consumer.build_kafka_consumer", return_value=fake_consumer),
        patch("consumers.fraud_consumer.build_schema_registry_client", return_value=object()),
        patch("consumers.fraud_consumer.AvroDeserializer", return_value=_fake_deser),
    ):
        stats, records = consume_fraud_alerts(
            env=_dummy_env(),
            db_path=db_file,
            max_messages=2,
            idle_timeout_sec=0.1,
            max_wait_sec=2.0,
        )

    assert fake_consumer.closed is True
    assert fake_consumer.committed_offsets == [0, 1]
    assert stats.invalid_messages == 1
    assert stats.applied_new == 1
    assert len(records) == 1


def test_consume_does_not_commit_offset_when_sqlite_fails(tmp_path: Path) -> None:
    fake_consumer = _FakeConsumer([_FakeMessage(b"ok", offset=7)])
    db_file = tmp_path / "fail.db"

    def _ok_deser(_raw: bytes | None, _ctx: Any) -> dict[str, Any]:
        return _sample_alert()

    with (
        patch("consumers.fraud_consumer.build_kafka_consumer", return_value=fake_consumer),
        patch("consumers.fraud_consumer.build_schema_registry_client", return_value=object()),
        patch("consumers.fraud_consumer.AvroDeserializer", return_value=_ok_deser),
        patch.object(
            FraudAlertRepository,
            "persist_validated_alert",
            side_effect=sqlite3.OperationalError("database is locked"),
        ),
        pytest.raises(sqlite3.OperationalError),
    ):
        consume_fraud_alerts(
            env=_dummy_env(),
            db_path=db_file,
            max_messages=1,
            idle_timeout_sec=0.1,
            max_wait_sec=2.0,
        )

    assert fake_consumer.committed_offsets == []
    assert fake_consumer.closed is True


def test_main_fails_gracefully_without_traceback_when_env_invalid(tmp_path: Path) -> None:
    fake_env_path = tmp_path / "missing.env"
    with patch("consumers.fraud_consumer.load_env", side_effect=FileNotFoundError(str(fake_env_path))):
        exit_code = consumer_main(["--max-wait", "1"])
    assert exit_code == 1


def test_trigger_burst_timestamps_fit_inside_60s_window(tmp_path: Path) -> None:
    anchor = datetime(2026, 11, 13, 7, 0, 0, tzinfo=UTC)
    stamps = build_burst_timestamps(base_utc=anchor, step_seconds=5)
    assert len(stamps) == len(BURST_AMOUNTS) == 3
    assert (stamps[-1] - stamps[0]).total_seconds() < 60

    proof = verify_idempotent_replay(
        {
            "topic": "desafio.fraud.detected",
            "partition": 0,
            "offset": 0,
            "key": "RULE_VELOCITY:42:2001",
            "value": _sample_alert(),
        },
        tmp_path / "test_replay_unit.db",
    )
    assert proof["first_delivery"] == "applied"
    assert proof["duplicate_redelivery"] == "duplicate"
    assert proof["db_counts_after_replay"] == {"processed_alerts": 1, "fraud_actions": 1}
    assert KafkaError._PARTITION_EOF != 0
