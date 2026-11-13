"""Consumidor idempotente de alertas `desafio.fraud.detected` com persistência atômica em SQLite."""

from __future__ import annotations

import argparse
import json
import logging
import signal
import sqlite3
import sys
import threading
import time
from dataclasses import dataclass
from datetime import UTC, datetime
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any, Literal, cast

from confluent_kafka import Consumer, KafkaError, TopicPartition
from confluent_kafka.schema_registry.avro import AvroDeserializer
from confluent_kafka.serialization import MessageField, SerializationContext, SerializationError

ROOT_DIR = Path(__file__).resolve().parent.parent
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from scripts.confluent_utils import (
    build_kafka_consumer,
    build_schema_registry_client,
    create_avro_deserializer,
    to_json_safe,
)
from scripts.env_utils import load_env

FRAUD_TOPIC = "desafio.fraud.detected"
DEFAULT_DB_PATH = ROOT_DIR / "data" / "fraud_alerts_sink.db"
DEFAULT_GROUP_ID = "desafio-fraud-consumer"
ACTION_STATUS = "BLOCK_CARD_REVIEW"
_REQUIRED_ENV_KEYS = (
    "KAFKA_BOOTSTRAP_SERVERS",
    "CONSUMER_API_KEY",
    "CONSUMER_API_SECRET",
    "SCHEMA_REGISTRY_URL",
    "SCHEMA_REGISTRY_API_KEY",
    "SCHEMA_REGISTRY_API_SECRET",
)

_KAFKA_SILENT_LOGGER = logging.getLogger("desafio.rdkafka.silent")
if not _KAFKA_SILENT_LOGGER.handlers:
    _KAFKA_SILENT_LOGGER.addHandler(logging.NullHandler())
_KAFKA_SILENT_LOGGER.propagate = False


def log_event(level: str, event: str, **fields: Any) -> None:
    """Emite log estruturado em JSON sem expor credenciais ou PII."""
    record = {
        "timestamp": datetime.now(UTC).isoformat(),
        "level": level,
        "service_name": "fraud-alert-consumer",
        "event": event,
        **to_json_safe(fields),
    }
    print(json.dumps(record, ensure_ascii=False), flush=True)


def validate_alert_payload(payload: object) -> dict[str, Any]:
    """Valida fronteira de entrada do payload Avro FraudAlert antes de persistir efeito colateral."""
    if not isinstance(payload, dict):
        raise TypeError("Payload FraudAlert deve ser um dicionário válido.")

    payload_map: dict[str, Any] = {str(k): v for k, v in cast(dict[Any, Any], payload).items()}

    required_fields = (
        "alert_id",
        "card_id",
        "account_id",
        "rule_name",
        "tx_count",
        "total_amount",
        "window_start",
        "window_end",
    )
    missing = [key for key in required_fields if payload_map.get(key) is None]
    if missing:
        raise ValueError(f"Campos obrigatórios ausentes em FraudAlert: {missing}")

    alert_id = str(payload_map["alert_id"]).strip()
    rule_name = str(payload_map["rule_name"]).strip()
    if not alert_id or not rule_name:
        raise ValueError("alert_id e rule_name devem ser strings não vazias.")

    card_id = int(payload_map["card_id"])
    account_id = int(payload_map["account_id"])
    tx_count = int(payload_map["tx_count"])
    if card_id <= 0 or account_id <= 0 or tx_count < 1:
        raise ValueError(
            f"Identificadores/contagem inválidos: card_id={card_id}, account_id={account_id}, tx_count={tx_count}"
        )

    try:
        amount = Decimal(str(payload_map["total_amount"]))
    except (InvalidOperation, ValueError) as exc:
        raise ValueError(f"total_amount inválido: {payload_map['total_amount']}") from exc
    if amount <= 0:
        raise ValueError(f"total_amount deve ser positivo: {amount}")

    return {
        "alert_id": alert_id,
        "card_id": card_id,
        "account_id": account_id,
        "rule_name": rule_name,
        "tx_count": tx_count,
        "total_amount": str(amount),
        "window_start": to_json_safe(payload_map["window_start"]),
        "window_end": to_json_safe(payload_map["window_end"]),
    }


@dataclass(frozen=True)
class ConsumerStats:
    processed_total: int
    applied_new: int
    duplicates_skipped: int
    ignored_tombstones: int
    invalid_messages: int


class FraudAlertRepository:
    """Repositório SQLite com deduplicação transacional por `alert_id` (exactly-once side-effect)."""

    def __init__(self, db_path: Path | str) -> None:
        self.db_path = Path(db_path)
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self._conn = sqlite3.connect(str(self.db_path), timeout=15.0)
        self._conn.execute("PRAGMA foreign_keys = ON;")
        self._conn.execute("PRAGMA journal_mode = WAL;")
        self._ensure_schema()

    def _ensure_schema(self) -> None:
        with self._conn:
            self._conn.execute(
                """
                CREATE TABLE IF NOT EXISTS processed_alerts (
                    alert_id TEXT PRIMARY KEY,
                    kafka_topic TEXT NOT NULL,
                    kafka_partition INTEGER NOT NULL,
                    kafka_offset INTEGER NOT NULL,
                    payload_json TEXT NOT NULL,
                    first_seen_at TEXT NOT NULL
                );
                """
            )
            self._conn.execute(
                """
                CREATE TABLE IF NOT EXISTS fraud_actions (
                    action_id INTEGER PRIMARY KEY AUTOINCREMENT,
                    alert_id TEXT NOT NULL UNIQUE
                        REFERENCES processed_alerts (alert_id) ON DELETE RESTRICT,
                    card_id INTEGER NOT NULL,
                    account_id INTEGER NOT NULL,
                    rule_name TEXT NOT NULL,
                    tx_count INTEGER NOT NULL CHECK (tx_count > 0),
                    total_amount TEXT NOT NULL,
                    window_start TEXT NOT NULL,
                    window_end TEXT NOT NULL,
                    action_status TEXT NOT NULL,
                    applied_at TEXT NOT NULL
                );
                """
            )

    def persist_validated_alert(
        self,
        normalized: dict[str, Any],
        topic: str,
        partition: int,
        offset: int,
    ) -> Literal["applied", "duplicate"]:
        """Persiste alerta já validado e dispara efeito colateral exatamente uma vez por alert_id."""
        now_utc = datetime.now(UTC).isoformat()
        payload_json = json.dumps(normalized, ensure_ascii=False, sort_keys=True)

        # Transação única garante atomicidade entre marcação de deduplicação e efeito colateral
        with self._conn:
            cur = self._conn.execute(
                """
                INSERT INTO processed_alerts (
                    alert_id, kafka_topic, kafka_partition, kafka_offset, payload_json, first_seen_at
                )
                VALUES (?, ?, ?, ?, ?, ?)
                ON CONFLICT(alert_id) DO NOTHING
                RETURNING alert_id;
                """,
                (normalized["alert_id"], topic, partition, offset, payload_json, now_utc),
            )
            inserted = cur.fetchone()
            if inserted is None:
                return "duplicate"

            self._conn.execute(
                """
                INSERT INTO fraud_actions (
                    alert_id, card_id, account_id, rule_name, tx_count,
                    total_amount, window_start, window_end, action_status, applied_at
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
                """,
                (
                    normalized["alert_id"],
                    normalized["card_id"],
                    normalized["account_id"],
                    normalized["rule_name"],
                    normalized["tx_count"],
                    normalized["total_amount"],
                    normalized["window_start"],
                    normalized["window_end"],
                    ACTION_STATUS,
                    now_utc,
                ),
            )
            return "applied"

    def apply_alert_idempotently(
        self,
        payload: object,
        topic: str,
        partition: int,
        offset: int,
    ) -> Literal["applied", "duplicate"]:
        """Valida o payload e persiste alerta de forma idempotente."""
        normalized = validate_alert_payload(payload)
        return self.persist_validated_alert(normalized, topic, partition, offset)

    def summary_counts(self) -> dict[str, int]:
        """Retorna contagem atual de alertas deduplicados e efeitos colaterais aplicados."""
        cur = self._conn.cursor()
        row_processed = cur.execute("SELECT COUNT(*) FROM processed_alerts;").fetchone()
        row_actions = cur.execute("SELECT COUNT(*) FROM fraud_actions;").fetchone()
        return {
            "processed_alerts": int(row_processed[0]) if row_processed else 0,
            "fraud_actions": int(row_actions[0]) if row_actions else 0,
        }

    def close(self) -> None:
        self._conn.close()


def decode_raw_key(raw_key: bytes | None) -> str | None:
    """Decodifica chave raw UTF-8 produzida pelo Flink SQL em `desafio.fraud.detected`."""
    if raw_key is None:
        return None
    return raw_key.decode("utf-8", errors="replace")


def consume_fraud_alerts(
    env: dict[str, str],
    db_path: Path | str = DEFAULT_DB_PATH,
    topic: str = FRAUD_TOPIC,
    group_id: str = DEFAULT_GROUP_ID,
    from_beginning: bool = False,
    min_offset: int | None = None,
    max_messages: int | None = None,
    idle_timeout_sec: float = 10.0,
    max_wait_sec: float = 60.0,
) -> tuple[ConsumerStats, list[dict[str, Any]]]:
    """Consome alertas de fraude com commit manual pós-transação e deduplicação local."""
    missing_env = [key for key in _REQUIRED_ENV_KEYS if not env.get(key, "").strip()]
    if missing_env:
        raise ValueError(f"Variáveis obrigatórias ausentes ou vazias no .env: {missing_env}")

    auto_reset = "earliest" if from_beginning else "latest"
    stop_requested = False
    fatal_kafka_error: str | None = None

    def _handle_sig(_signum: int, _frame: Any) -> None:
        nonlocal stop_requested
        stop_requested = True

    def _on_kafka_error(err: KafkaError) -> None:
        nonlocal fatal_kafka_error
        fatal_codes = {
            KafkaError._AUTHENTICATION,
            KafkaError._ALL_BROKERS_DOWN,
            KafkaError._RESOLVE,
        }
        if fatal_kafka_error is None and (err.fatal() or err.code() in fatal_codes):
            fatal_kafka_error = f"{err.name()}: {err.str()}"

    is_main_thread = threading.current_thread() is threading.main_thread()
    prev_sigint = signal.getsignal(signal.SIGINT) if is_main_thread else None
    prev_sigterm = signal.getsignal(signal.SIGTERM) if is_main_thread else None

    captured_records: list[dict[str, Any]] = []
    processed_total = 0
    applied_new = 0
    duplicates_skipped = 0
    ignored_tombstones = 0
    invalid_messages = 0

    consumer: Consumer | None = None
    repo: FraudAlertRepository | None = None

    try:
        if is_main_thread:
            signal.signal(signal.SIGINT, _handle_sig)
            signal.signal(signal.SIGTERM, _handle_sig)

        active_consumer = build_kafka_consumer(
            env=env,
            group_id=group_id,
            auto_offset_reset=auto_reset,
            enable_auto_commit=False,
            error_cb=_on_kafka_error,
            logger=_KAFKA_SILENT_LOGGER,
        )
        consumer = active_consumer
        sr_client = build_schema_registry_client(env)
        val_deserializer = create_avro_deserializer(sr_client, AvroDeserializer)
        active_repo = FraudAlertRepository(db_path)
        repo = active_repo

        def _on_assign(c: Consumer, partitions: list[TopicPartition]) -> None:
            if min_offset is not None:
                c.assign([TopicPartition(tp.topic, tp.partition, min_offset) for tp in partitions])
            else:
                c.assign(partitions)

        active_consumer.subscribe([topic], on_assign=_on_assign)
        assign_deadline = time.monotonic() + 20.0
        while not active_consumer.assignment() and not stop_requested:
            active_consumer.poll(0.2)
            if fatal_kafka_error is not None:
                raise RuntimeError(f"Falha ao conectar/autenticar no cluster Kafka: {fatal_kafka_error}")
            if time.monotonic() > assign_deadline:
                raise TimeoutError(f"Timeout aguardando atribuição de partições em '{topic}'.")

        start = time.monotonic()
        last_activity = time.monotonic()
        while not stop_requested and time.monotonic() - start < max_wait_sec:
            if max_messages is not None and processed_total >= max_messages:
                break

            msg = active_consumer.poll(1.0)
            if fatal_kafka_error is not None:
                raise RuntimeError(f"Conexão Kafka interrompida: {fatal_kafka_error}")
            if msg is None:
                has_progress = bool(captured_records) or processed_total > 0
                if has_progress and time.monotonic() - last_activity >= idle_timeout_sec:
                    break
                continue

            err = msg.error()
            if err is not None:
                if err.code() == KafkaError._PARTITION_EOF:
                    if time.monotonic() - last_activity >= idle_timeout_sec:
                        break
                    continue
                raise RuntimeError(f"Erro Kafka no consumidor de fraude: {err}")

            last_activity = time.monotonic()
            processed_total += 1
            raw_partition = msg.partition()
            raw_offset = msg.offset()
            if raw_partition is None or raw_offset is None:
                raise RuntimeError("Mensagem Kafka sem partition ou offset válidos.")
            msg_topic = str(msg.topic() or topic)
            msg_partition = int(raw_partition)
            msg_offset = int(raw_offset)

            raw_value = msg.value()
            if raw_value is None:
                ignored_tombstones += 1
                active_consumer.commit(message=msg, asynchronous=False)
                continue

            # Isola apenas erros permanentes de desserialização/contrato (poison pill)
            # Falhas de persistência SQLite NÃO devem comitar o offset para evitar perda silenciosa.
            try:
                decoded_val = val_deserializer(
                    raw_value, SerializationContext(topic, MessageField.VALUE)
                )
                normalized_val = validate_alert_payload(decoded_val)
            except (SerializationError, TypeError, ValueError, KeyError) as exc:
                invalid_messages += 1
                log_event(
                    "ERROR",
                    "alert_poison_pill_skipped",
                    topic=msg_topic,
                    partition=msg_partition,
                    offset=msg_offset,
                    error=str(exc),
                )
                active_consumer.commit(message=msg, asynchronous=False)
                continue

            outcome = active_repo.persist_validated_alert(
                normalized=normalized_val,
                topic=msg_topic,
                partition=msg_partition,
                offset=msg_offset,
            )

            if outcome == "applied":
                applied_new += 1
            else:
                duplicates_skipped += 1

            # Commit manual somente após transação ACID no SQLite para evitar perda em falhas
            active_consumer.commit(message=msg, asynchronous=False)

            alert_record = {
                "topic": msg_topic,
                "partition": msg_partition,
                "offset": msg_offset,
                "key": decode_raw_key(msg.key()),
                "value": normalized_val,
                "idempotency_outcome": outcome,
            }
            captured_records.append(alert_record)
            log_event(
                "INFO",
                "fraud_alert_consumed",
                alert_id=normalized_val["alert_id"],
                card_id=normalized_val["card_id"],
                tx_count=normalized_val["tx_count"],
                outcome=outcome,
                offset=msg_offset,
            )
    finally:
        if is_main_thread and prev_sigint is not None and prev_sigterm is not None:
            signal.signal(signal.SIGINT, prev_sigint)
            signal.signal(signal.SIGTERM, prev_sigterm)
        if consumer is not None:
            consumer.close()
        if repo is not None:
            repo.close()

    stats = ConsumerStats(
        processed_total=processed_total,
        applied_new=applied_new,
        duplicates_skipped=duplicates_skipped,
        ignored_tombstones=ignored_tombstones,
        invalid_messages=invalid_messages,
    )
    return stats, captured_records


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Consumidor idempotente de alertas de fraude.")
    parser.add_argument("--from-beginning", action="store_true", help="Lê do offset inicial se grupo for novo.")
    parser.add_argument("--group-id", default=DEFAULT_GROUP_ID, help="Consumer group ID.")
    parser.add_argument("--db-path", type=Path, default=DEFAULT_DB_PATH, help="Caminho do SQLite de deduplicação.")
    parser.add_argument("--max-messages", type=int, default=None, help="Limite opcional de mensagens.")
    parser.add_argument("--idle-timeout", type=float, default=10.0, help="Segundos ociosos para encerrar.")
    parser.add_argument("--max-wait", type=float, default=60.0, help="Timeout total de leitura em segundos.")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    try:
        env = load_env(ROOT_DIR / ".env")
        stats, _ = consume_fraud_alerts(
            env=env,
            db_path=args.db_path,
            group_id=args.group_id,
            from_beginning=args.from_beginning,
            max_messages=args.max_messages,
            idle_timeout_sec=args.idle_timeout,
            max_wait_sec=args.max_wait,
        )
    except (FileNotFoundError, KeyError, ValueError, RuntimeError, TimeoutError) as exc:
        log_event("ERROR", "fraud_consumer_aborted", error=str(exc))
        print(
            "Dica: se o ambiente Confluent Cloud já foi desmontado com `./teardown.sh`, "
            "execute `./setup.sh` novamente para provisionar cluster e credenciais antes de consumir.",
            file=sys.stderr,
        )
        return 1
    print(f"FRAUD_CONSUMER_DONE: {stats}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
