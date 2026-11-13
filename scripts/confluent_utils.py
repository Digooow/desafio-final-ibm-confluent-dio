"""Utilitários compartilhados de CLI, Kafka Client, Schema Registry e serialização JSON."""

from __future__ import annotations

import logging
import shutil
import subprocess
from collections.abc import Callable
from datetime import date, datetime
from decimal import Decimal
from pathlib import Path
from typing import Any, cast

from confluent_kafka import Consumer, KafkaError
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer
from confluent_kafka.serialization import SerializationContext

AvroDecodeCallable = Callable[[bytes | None, SerializationContext | None], object]


def resolve_confluent_bin() -> str:
    """Resolve o binário do Confluent CLI no PATH ou no diretório padrão do WinGet."""
    found = shutil.which("confluent")
    if found:
        return found
    winget_candidate = (
        Path.home()
        / "AppData/Local/Microsoft/WinGet/Packages"
        / "ConfluentInc.Confluent-CLI_Microsoft.Winget.Source_8wekyb3d8bbwe"
        / "confluent/confluent.exe"
    )
    if winget_candidate.is_file():
        return str(winget_candidate)
    raise FileNotFoundError("Binário 'confluent' não encontrado no PATH.")


def run_confluent(cli: str, args: list[str]) -> subprocess.CompletedProcess[str]:
    """Executa subcomando da Confluent CLI sem expor segredos via shell."""
    return subprocess.run(
        [cli, *args],
        check=False,
        capture_output=True,
        text=True,
        encoding="utf-8",
    )


def build_kafka_consumer(
    env: dict[str, str],
    group_id: str,
    auto_offset_reset: str = "earliest",
    enable_auto_commit: bool = False,
    error_cb: Callable[[KafkaError], None] | None = None,
    logger: logging.Logger | None = None,
) -> Consumer:
    """Constroi Consumer Kafka autenticado via SASL_SSL usando CONSUMER_API_KEY."""
    conf: dict[str, Any] = {
        "bootstrap.servers": env["KAFKA_BOOTSTRAP_SERVERS"].strip(),
        "security.protocol": "SASL_SSL",
        "sasl.mechanisms": "PLAIN",
        "sasl.username": env["CONSUMER_API_KEY"].strip(),
        "sasl.password": env["CONSUMER_API_SECRET"].strip(),
        "group.id": group_id,
        "auto.offset.reset": auto_offset_reset,
        "enable.auto.commit": enable_auto_commit,
    }
    if error_cb is not None:
        conf["error_cb"] = error_cb
    if logger is not None:
        return Consumer(conf, logger=logger)
    return Consumer(conf)


def build_schema_registry_client(env: dict[str, str]) -> SchemaRegistryClient:
    """Constroi SchemaRegistryClient autenticado sem vazar credenciais."""
    return SchemaRegistryClient(
        {
            "url": env["SCHEMA_REGISTRY_URL"].strip(),
            "basic.auth.user.info": (
                f"{env['SCHEMA_REGISTRY_API_KEY'].strip()}:{env['SCHEMA_REGISTRY_API_SECRET'].strip()}"
            ),
        }
    )


def create_avro_deserializer(
    sr_client: SchemaRegistryClient,
    deserializer_cls: Any = AvroDeserializer,
) -> AvroDecodeCallable:
    """Instancia AvroDeserializer preservando compatibilidade com patches de teste e tipagem estrita."""
    factory = cast(Callable[[SchemaRegistryClient], AvroDecodeCallable], deserializer_cls)
    return factory(sr_client)


def to_json_safe(obj: Any) -> Any:
    """Converte tipos Avro/Python (Decimal, datetime, bytes) para JSON preservando precisão."""
    if obj is None or isinstance(obj, (bool, int, float, str)):
        return obj
    if isinstance(obj, Decimal):
        return str(obj)
    if isinstance(obj, (datetime, date)):
        return obj.isoformat()
    if isinstance(obj, (bytes, bytearray)):
        return "0x" + bytes(obj).hex()
    if isinstance(obj, dict):
        dict_obj = cast(dict[Any, Any], obj)
        return {str(k): to_json_safe(v) for k, v in dict_obj.items()}
    if isinstance(obj, (list, tuple)):
        seq_obj = cast(list[Any] | tuple[Any, ...], obj)
        return [to_json_safe(item) for item in seq_obj]
    return str(obj)
