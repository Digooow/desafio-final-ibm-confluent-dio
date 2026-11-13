"""Testes unitários de segurança e contrato da configuração CDC (connectors/cdc.json)."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from scripts.deploy_cdc import TOPIC_SPECS, render_connector_config

ROOT_DIR = Path(__file__).resolve().parent.parent


def test_cdc_json_uses_placeholders_and_required_debezium_settings() -> None:
    payload = json.loads((ROOT_DIR / "connectors" / "cdc.json").read_text(encoding="utf-8"))
    cfg = payload["config"]

    assert cfg["connector.class"] == "PostgresCdcSourceV2"
    assert cfg["database.user"] == "${PG_CDC_USER}"
    assert cfg["database.password"] == "${PG_CDC_PASSWORD}"
    assert cfg["kafka.api.key"] == "${PRODUCER_API_KEY}"
    assert cfg["kafka.api.secret"] == "${PRODUCER_API_SECRET}"
    assert cfg["database.hostname"] == "${PG_HOST}"
    assert cfg["database.sslmode"] == "${PG_SSLMODE}"
    assert cfg["heartbeat.interval.ms"] == "60000"
    assert cfg["topic.heartbeat.prefix"] == "desafio.cdc.heartbeat"
    assert cfg["after.state.only"] == "false"
    assert cfg["tombstones.on.delete"] == "true"
    assert cfg["publication.autocreate.mode"] == "disabled"
    assert cfg["decimal.handling.mode"] == "precise"
    assert cfg["output.data.format"] == "AVRO"
    assert cfg["output.key.format"] == "AVRO"


def test_render_connector_config_fails_closed_on_missing_env() -> None:
    template = json.loads((ROOT_DIR / "connectors" / "cdc.json").read_text(encoding="utf-8"))
    with pytest.raises(ValueError, match="Variáveis obrigatórias ausentes"):
        render_connector_config(template, {"PG_HOST": "example.neon.tech"})


def test_topic_specs_respect_acl_prefix() -> None:
    for topic_name, partitions, policy in TOPIC_SPECS:
        assert topic_name.startswith("desafio.")
        assert partitions == 1
        assert policy in {"compact", "delete"}
