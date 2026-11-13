"""Dispara 3 transações APPROVED em <60s no Neon e captura alerta Flink + prova de idempotência."""

from __future__ import annotations

import json
import sys
import uuid
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path
from typing import Any

import psycopg
from confluent_kafka import TopicPartition

ROOT_DIR = Path(__file__).resolve().parent.parent
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from consumers.fraud_consumer import (
    FRAUD_TOPIC,
    FraudAlertRepository,
    consume_fraud_alerts,
)
from scripts.confluent_utils import build_kafka_consumer, to_json_safe
from scripts.env_utils import load_env

BURST_AMOUNTS: tuple[Decimal, ...] = (
    Decimal("185.40"),
    Decimal("319.90"),
    Decimal("642.15"),
)


@dataclass(frozen=True)
class CandidateCard:
    account_id: int
    card_id: int
    merchant_id: int


def select_clean_active_card(conn: psycopg.Connection) -> CandidateCard:
    """Seleciona um cartão ACTIVE vinculado a conta ACTIVE sem transações recentes (<5min)."""
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT a.account_id, c.card_id
            FROM public.cards AS c
            JOIN public.accounts AS a ON a.account_id = c.account_id
            WHERE a.status = 'ACTIVE'
              AND c.status = 'ACTIVE'
              AND NOT EXISTS (
                  SELECT 1
                  FROM public.transactions AS t
                  WHERE t.card_id = c.card_id
                    AND t.created_at >= timezone('utc', clock_timestamp()) - INTERVAL '5 minutes'
              )
            ORDER BY c.card_id ASC
            LIMIT 1;
            """
        )
        row = cur.fetchone()
        if row is None:
            raise RuntimeError("Nenhum par (account ACTIVE, card ACTIVE) elegível encontrado.")
        account_id, card_id = int(row[0]), int(row[1])

        cur.execute("SELECT merchant_id FROM public.merchants ORDER BY merchant_id ASC LIMIT 1;")
        m_row = cur.fetchone()
        if m_row is None:
            raise RuntimeError("Nenhum merchant cadastrado para disparar transações.")
        merchant_id = int(m_row[0])

    return CandidateCard(account_id=account_id, card_id=card_id, merchant_id=merchant_id)


def build_burst_timestamps(base_utc: datetime | None = None, step_seconds: int = 5) -> list[datetime]:
    """Gera 3 timestamps UTC dentro de uma janela <60s para disparar RULE_VELOCITY."""
    anchor = (base_utc or datetime.now(UTC)).replace(tzinfo=None)
    return [anchor + timedelta(seconds=idx * step_seconds) for idx in range(len(BURST_AMOUNTS))]


def insert_velocity_burst(
    conn: psycopg.Connection,
    candidate: CandidateCard,
    timestamps: list[datetime],
) -> list[dict[str, Any]]:
    """Insere 3 transações APPROVED em commits sequenciais para gerar eventos CDC reais."""
    inserted: list[dict[str, Any]] = []
    channels = ("POS", "ECOMMERCE", "CONTACTLESS")

    for amount, ts, channel in zip(BURST_AMOUNTS, timestamps, channels, strict=True):
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO public.transactions (
                    account_id, card_id, merchant_id, amount, currency, status, channel, created_at
                )
                VALUES (%s, %s, %s, %s, 'BRL', 'APPROVED', %s, %s)
                RETURNING transaction_id, account_id, card_id, merchant_id, amount, currency, status, channel, created_at;
                """,
                (candidate.account_id, candidate.card_id, candidate.merchant_id, amount, channel, ts),
            )
            row = cur.fetchone()
            if row is None:
                raise RuntimeError("INSERT RETURNING em public.transactions não retornou linha.")
        conn.commit()
        inserted.append(
            {
                "transaction_id": int(row[0]),
                "account_id": int(row[1]),
                "card_id": int(row[2]),
                "merchant_id": int(row[3]),
                "amount": str(row[4]),
                "currency": row[5],
                "status": row[6],
                "channel": row[7],
                "created_at": to_json_safe(row[8]),
            }
        )
    return inserted


def _get_high_watermark(env: dict[str, str], topic: str) -> int:
    probe_consumer = build_kafka_consumer(
        env=env,
        group_id=f"desafio-fraud-hwm-{uuid.uuid4().hex[:6]}",
        auto_offset_reset="latest",
    )
    try:
        _, high = probe_consumer.get_watermark_offsets(TopicPartition(topic, 0), timeout=15.0)
        return int(high)
    finally:
        probe_consumer.close()


def verify_idempotent_replay(alert_record: dict[str, Any], db_path: Path) -> dict[str, Any]:
    """Reprocessa o mesmo alerta capturado para provar deduplicação exata por `alert_id`."""
    if db_path.exists():
        db_path.unlink()
    repo = FraudAlertRepository(db_path)
    try:
        first_outcome = repo.apply_alert_idempotently(
            payload=alert_record["value"],
            topic=alert_record["topic"],
            partition=alert_record["partition"],
            offset=alert_record["offset"],
        )
        second_outcome = repo.apply_alert_idempotently(
            payload=alert_record["value"],
            topic=alert_record["topic"],
            partition=alert_record["partition"],
            offset=alert_record["offset"],
        )
        counts = repo.summary_counts()
    finally:
        repo.close()

    if first_outcome != "applied" or second_outcome != "duplicate":
        raise AssertionError(
            f"Comportamento idempotente inesperado: 1o={first_outcome}, 2o={second_outcome}"
        )
    if counts != {"processed_alerts": 1, "fraud_actions": 1}:
        raise AssertionError(f"Contagem esperada 1/1 após replay duplicado, obtido={counts}")

    return {
        "first_delivery": first_outcome,
        "duplicate_redelivery": second_outcome,
        "db_counts_after_replay": counts,
    }


def main() -> int:
    env = load_env(ROOT_DIR / ".env")
    db_url = env["DATABASE_URL_UNPOOLED"].strip()

    high_before = _get_high_watermark(env, FRAUD_TOPIC)
    print(f"[info] High watermark inicial em {FRAUD_TOPIC}: offset={high_before}")

    with psycopg.connect(db_url, connect_timeout=20) as conn:
        candidate = select_clean_active_card(conn)
        timestamps = build_burst_timestamps()
        print(
            f"[info] Disparando 3 transações APPROVED no card_id={candidate.card_id}, account_id={candidate.account_id}..."
        )
        trigger_txs = insert_velocity_burst(conn, candidate, timestamps)

    # Aguarda propagação Debezium -> Kafka -> Flink SQL (idle-timeout=5s + watermark 5s) -> tópico de fraude
    sink_db = ROOT_DIR / "data" / "fraud_alerts_sink.db"
    stats, alerts = consume_fraud_alerts(
        env=env,
        db_path=sink_db,
        topic=FRAUD_TOPIC,
        group_id=f"desafio-fraud-verify-{uuid.uuid4().hex[:6]}",
        from_beginning=True,
        min_offset=high_before,
        idle_timeout_sec=15.0,
        max_wait_sec=120.0,
    )

    matching = [
        item for item in alerts if int(item["value"]["card_id"]) == candidate.card_id
    ]
    if not matching:
        raise TimeoutError(
            f"Nenhum alerta emitido para card_id={candidate.card_id} após 120s. "
            f"Verifique se o job Flink está RUNNING e se a watermark avançou. Capturados={len(alerts)}"
        )

    matched_alert = matching[0]
    idempotency_proof = verify_idempotent_replay(
        matched_alert, ROOT_DIR / "data" / "idempotency_replay_test.db"
    )

    evidence = {
        "captured_at_utc": datetime.now(UTC).isoformat(),
        "rule": "RULE_VELOCITY (>= 3 APPROVED tx em <= 60s para mesmo card_id em account ACTIVE)",
        "trigger_transactions": trigger_txs,
        "fraud_alert": matched_alert,
        "live_consumer_stats": {
            "processed_total": stats.processed_total,
            "applied_new": stats.applied_new,
            "duplicates_skipped": stats.duplicates_skipped,
        },
        "idempotency_verification": idempotency_proof,
    }

    evidence_path = ROOT_DIR / "evidencias" / "02-alerta-fraude.json"
    evidence_path.parent.mkdir(parents=True, exist_ok=True)
    evidence_path.write_text(json.dumps(evidence, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")
    print(f"FRAUD_BURST_VERIFIED: {evidence_path} | alert_id={matched_alert['value']['alert_id']}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
