"""Verifica snapshot inicial e captura envelopes op=c, op=u, op=d + tombstone em evidencias/01-envelopes-cdc.json."""

from __future__ import annotations

import json
import sys
import time
import uuid
from pathlib import Path
from typing import Any, cast

import psycopg
from confluent_kafka import KafkaError, TopicPartition
from confluent_kafka.schema_registry.avro import AvroDeserializer
from confluent_kafka.serialization import MessageField, SerializationContext

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

CDC_TOPICS = (
    "desafio.cdc.public.customers",
    "desafio.cdc.public.accounts",
    "desafio.cdc.public.cards",
    "desafio.cdc.public.merchants",
    "desafio.cdc.public.transactions",
)


def classify_cdc_record(key: dict[str, Any] | None, value: dict[str, Any] | None) -> str:
    """Classifica um registro CDC Debezium como create, update, delete, tombstone ou snapshot."""
    if value is None:
        return "tombstone"
    op = value.get("op")
    if op == "c":
        return "create"
    if op == "u":
        return "update"
    if op == "d":
        return "delete"
    if op == "r":
        return "read"
    return f"other:{op}"


def count_topic_messages(
    env: dict[str, str],
    topics: tuple[str, ...] = CDC_TOPICS,
    idle_timeout_sec: float = 5.0,
    max_wait_sec: float = 60.0,
) -> dict[str, int]:
    """Conta mensagens visíveis em cada tópico CDC a partir do offset inicial."""
    group_id = f"desafio-cdc-count-{uuid.uuid4().hex[:8]}"
    consumer = build_kafka_consumer(env, group_id)
    counts = {topic: 0 for topic in topics}
    try:
        consumer.subscribe(list(topics))
        start = time.monotonic()
        last_msg = time.monotonic()
        while time.monotonic() - start < max_wait_sec:
            msg = consumer.poll(1.0)
            if msg is None:
                if time.monotonic() - last_msg >= idle_timeout_sec:
                    break
                continue
            err = msg.error()
            if err is not None:
                if err.code() == KafkaError._PARTITION_EOF:
                    continue
                raise RuntimeError(f"Erro ao consumir tópico: {err}")
            topic = msg.topic()
            if topic in counts:
                counts[topic] += 1
            last_msg = time.monotonic()
    finally:
        consumer.close()
    return counts


def capture_crud_envelopes(
    env: dict[str, str],
    timeout_sec: float = 90.0,
) -> dict[str, Any]:
    """Executa INSERT -> UPDATE -> DELETE no Neon e captura op=c, op=u, op=d e tombstone."""
    topic = "desafio.cdc.public.customers"
    group_id = f"desafio-cdc-verify-{uuid.uuid4().hex[:8]}"
    consumer = build_kafka_consumer(env, group_id)
    sr_client = build_schema_registry_client(env)
    key_deser = create_avro_deserializer(sr_client, AvroDeserializer)
    val_deser = create_avro_deserializer(sr_client, AvroDeserializer)

    probe_tag = uuid.uuid4().hex[:6]
    probe_email = f"cdc.probe.{probe_tag}@example.test"
    probe_doc = f"***-CDC{probe_tag}-**"
    captured: dict[str, Any] = {}
    probe_id: int | None = None

    try:
        # Posiciona consumer explicitamente no high watermark atual antes das mutações ao vivo
        consumer.subscribe([topic])
        deadline_assign = time.monotonic() + 20.0
        assignment = consumer.assignment()
        while not assignment:
            consumer.poll(0.2)
            assignment = consumer.assignment()
            if time.monotonic() > deadline_assign:
                raise TimeoutError("Timeout aguardando atribuição de partição no consumer.")
        for tp in assignment:
            _, high = consumer.get_watermark_offsets(tp, timeout=10.0)
            consumer.seek(TopicPartition(tp.topic, tp.partition, high))

        with psycopg.connect(env["DATABASE_URL_UNPOOLED"].strip(), connect_timeout=20) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO public.customers (full_name, email, document_id, risk_tier)
                    VALUES (%s, %s, %s, 'LOW')
                    RETURNING customer_id;
                    """,
                    (f"Cliente CDC Teste {probe_tag}", probe_email, probe_doc),
                )
                row = cur.fetchone()
                if row is None:
                    raise RuntimeError("INSERT RETURNING em public.customers não retornou linha.")
                probe_id = int(row[0])
            conn.commit()

            with conn.cursor() as cur:
                cur.execute(
                    """
                    UPDATE public.customers
                    SET risk_tier = 'HIGH', full_name = %s
                    WHERE customer_id = %s;
                    """,
                    (f"Cliente CDC Atualizado {probe_tag}", probe_id),
                )
            conn.commit()

            with conn.cursor() as cur:
                cur.execute("DELETE FROM public.customers WHERE customer_id = %s;", (probe_id,))
            conn.commit()

        deadline = time.monotonic() + timeout_sec
        needed = {"create", "update", "delete", "tombstone"}
        while set(captured.keys()) != needed and time.monotonic() < deadline:
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                raise RuntimeError(f"Erro Kafka durante captura CDC: {msg.error()}")

            decoded_key_raw = (
                key_deser(msg.key(), SerializationContext(topic, MessageField.KEY))
                if msg.key() is not None
                else None
            )
            decoded_val_raw = (
                val_deser(msg.value(), SerializationContext(topic, MessageField.VALUE))
                if msg.value() is not None
                else None
            )

            decoded_key: dict[str, Any] | None = (
                {str(k): v for k, v in cast(dict[Any, Any], decoded_key_raw).items()}
                if isinstance(decoded_key_raw, dict)
                else None
            )
            decoded_val: dict[str, Any] | None = (
                {str(k): v for k, v in cast(dict[Any, Any], decoded_val_raw).items()}
                if isinstance(decoded_val_raw, dict)
                else None
            )

            key_id = decoded_key.get("customer_id") if decoded_key is not None else None
            if key_id != probe_id:
                continue

            kind = classify_cdc_record(decoded_key, decoded_val)
            if kind in needed and kind not in captured:
                captured[kind] = {
                    "topic": msg.topic(),
                    "partition": msg.partition(),
                    "offset": msg.offset(),
                    "key": to_json_safe(decoded_key),
                    "value": to_json_safe(decoded_val),
                }

        missing = needed - set(captured.keys())
        if missing:
            raise TimeoutError(
                f"Não capturou todos os eventos CDC para customer_id={probe_id}; faltaram: {sorted(missing)}"
            )

        # Valida invariantes explícitas de REPLICA IDENTITY FULL e tombstones
        assert captured["create"]["value"]["before"] is None
        assert captured["create"]["value"]["after"]["customer_id"] == probe_id
        assert captured["update"]["value"]["before"] is not None
        assert captured["update"]["value"]["before"]["risk_tier"] == "LOW"
        assert captured["update"]["value"]["after"]["risk_tier"] == "HIGH"
        assert captured["delete"]["value"]["before"] is not None
        assert captured["delete"]["value"]["after"] is None
        assert captured["tombstone"]["key"]["customer_id"] == probe_id
        assert captured["tombstone"]["value"] is None

        return {
            "probe_customer_id": probe_id,
            "events": {
                "op_c_insert": captured["create"],
                "op_u_update": captured["update"],
                "op_d_delete": captured["delete"],
                "tombstone": captured["tombstone"],
            },
        }
    finally:
        consumer.close()


def main() -> int:
    env = load_env(ROOT_DIR / ".env")
    snapshot_counts = count_topic_messages(env)
    print("Snapshot topic counts:", json.dumps(snapshot_counts, indent=2))

    evidence = capture_crud_envelopes(env)
    evidence["snapshot_topic_counts"] = snapshot_counts

    out_path = ROOT_DIR / "evidencias" / "01-envelopes-cdc.json"
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(json.dumps(evidence, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")
    print(f"CDC_EVIDENCE_SAVED: {out_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
