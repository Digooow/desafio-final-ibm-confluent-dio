"""Provisiona tópicos CDC e implanta o conector PostgresCdcSourceV2 sem expor segredos."""

from __future__ import annotations

import contextlib
import json
import os
import re
import sys
import tempfile
import time
from pathlib import Path
from typing import Any, cast

ROOT_DIR = Path(__file__).resolve().parent.parent
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from scripts.confluent_utils import resolve_confluent_bin, run_confluent
from scripts.env_utils import load_env

PLACEHOLDER_RE = re.compile(r"\$\{([A-Z0-9_]+)\}")

TOPIC_SPECS: tuple[tuple[str, int, str], ...] = (
    ("desafio.cdc.public.customers", 1, "compact"),
    ("desafio.cdc.public.accounts", 1, "compact"),
    ("desafio.cdc.public.cards", 1, "compact"),
    ("desafio.cdc.public.merchants", 1, "compact"),
    ("desafio.cdc.public.transactions", 1, "delete"),
    ("desafio.cdc.heartbeat.desafio.cdc", 1, "delete"),
    ("desafio.fraud.detected", 1, "delete"),
)


def render_connector_config(template: dict[str, Any], env: dict[str, str]) -> dict[str, Any]:
    """Substitui placeholders ${VAR} estritamente a partir do dicionário `env`."""
    missing: set[str] = set()

    def _substitute(value: Any) -> Any:
        if isinstance(value, str):
            def _repl(match: re.Match[str]) -> str:
                var = match.group(1)
                resolved = env.get(var, "").strip()
                if not resolved:
                    missing.add(var)
                    return ""
                return resolved

            return PLACEHOLDER_RE.sub(_repl, value)
        if isinstance(value, dict):
            dict_val = cast(dict[Any, Any], value)
            return {str(k): _substitute(v) for k, v in dict_val.items()}
        if isinstance(value, list):
            list_val = cast(list[Any], value)
            return [_substitute(item) for item in list_val]
        return value

    rendered = cast(dict[str, Any], _substitute(template))
    if missing:
        raise ValueError(f"Variáveis obrigatórias ausentes no .env: {sorted(missing)}")
    return rendered


def ensure_topics(cli: str, env_id: str, cluster_id: str) -> None:
    """Cria tópicos CDC/fraude com particionamento e cleanup.policy explícitos (menor privilégio para o conector)."""
    res = run_confluent(
        cli,
        ["kafka", "topic", "list", "--environment", env_id, "--cluster", cluster_id, "-o", "json"],
    )
    if res.returncode != 0:
        raise RuntimeError(f"Falha ao listar tópicos Kafka: {res.stderr.strip()}")

    topic_items = cast(list[dict[str, Any]], json.loads(res.stdout or "[]") or [])
    existing_topics = {str(item["name"]) for item in topic_items}
    for topic_name, partitions, cleanup_policy in TOPIC_SPECS:
        if topic_name in existing_topics:
            print(f"[ok] Tópico existente: {topic_name}")
            continue
        create_res = run_confluent(
            cli,
            [
                "kafka",
                "topic",
                "create",
                topic_name,
                "--partitions",
                str(partitions),
                "--config",
                f"cleanup.policy={cleanup_policy}",
                "--environment",
                env_id,
                "--cluster",
                cluster_id,
            ],
        )
        if create_res.returncode != 0:
            raise RuntimeError(
                f"Falha ao criar tópico {topic_name}: {create_res.stderr.strip()}"
            )
        print(f"[ok] Tópico criado: {topic_name} (partitions={partitions}, cleanup.policy={cleanup_policy})")


def find_connector_id(cli: str, name: str, env_id: str, cluster_id: str) -> str | None:
    """Retorna o ID do conector se ele já existir no cluster."""
    res = run_confluent(
        cli,
        ["connect", "cluster", "list", "--environment", env_id, "--cluster", cluster_id, "-o", "json"],
    )
    if res.returncode != 0:
        raise RuntimeError(f"Falha ao listar conectores: {res.stderr.strip()}")
    connectors = cast(list[dict[str, Any]], json.loads(res.stdout or "[]") or [])
    for item in connectors:
        if item.get("name") == name:
            item_id = item.get("id")
            return str(item_id) if item_id is not None else None
    return None


def wait_connector_running(
    cli: str,
    connector_id: str,
    env_id: str,
    cluster_id: str,
    max_attempts: int = 30,
    interval_sec: int = 10,
) -> str:
    """Aguarda o conector atingir o estado RUNNING com limite finito de iterações."""
    for attempt in range(1, max_attempts + 1):
        res = run_confluent(
            cli,
            [
                "connect",
                "cluster",
                "describe",
                connector_id,
                "--environment",
                env_id,
                "--cluster",
                cluster_id,
                "-o",
                "json",
            ],
        )
        if res.returncode != 0:
            raise RuntimeError(f"Erro ao descrever conector {connector_id}: {res.stderr.strip()}")
        info = cast(dict[str, Any], json.loads(res.stdout))
        status_obj: Any = (
            info.get("status")
            or cast(dict[str, Any], info.get("connector") or {}).get("status")
            or cast(dict[str, Any], info.get("info") or {}).get("status")
            or "UNKNOWN"
        )
        if isinstance(status_obj, dict):
            status_str = str(cast(dict[str, Any], status_obj).get("state", "UNKNOWN"))
        else:
            status_str = str(status_obj)
        print(f"[{attempt}/{max_attempts}] Conector {connector_id} status={status_str}")
        if status_str.upper() == "RUNNING":
            return status_str
        if status_str.upper() in {"FAILED", "DEGRADED"}:
            raise RuntimeError(f"Conector entrou em estado de erro: {res.stdout}")
        if attempt < max_attempts:
            time.sleep(interval_sec)
    raise TimeoutError(f"Timeout aguardando conector {connector_id} atingir RUNNING.")


def main() -> int:
    env = load_env(ROOT_DIR / ".env")
    env_id = env["CONFLUENT_ENV_ID"].strip()
    cluster_id = env["CONFLUENT_CLUSTER_ID"].strip()
    cli = resolve_confluent_bin()

    template = cast(
        dict[str, Any], json.loads((ROOT_DIR / "connectors" / "cdc.json").read_text(encoding="utf-8"))
    )
    connector_name = str(template["name"])

    ensure_topics(cli, env_id, cluster_id)

    connector_id = find_connector_id(cli, connector_name, env_id, cluster_id)
    if connector_id:
        print(f"[ok] Conector existente: {connector_name} ({connector_id})")
    else:
        rendered = render_connector_config(template, env)
        with tempfile.TemporaryDirectory() as tmpdir:
            tmp_file = Path(tmpdir) / "cdc.rendered.json"
            tmp_file.write_text(json.dumps(rendered, indent=2), encoding="utf-8")
            with contextlib.suppress(OSError):
                os.chmod(tmp_file, 0o600)
            create_res = run_confluent(
                cli,
                [
                    "connect",
                    "cluster",
                    "create",
                    "--config-file",
                    str(tmp_file),
                    "--environment",
                    env_id,
                    "--cluster",
                    cluster_id,
                    "-o",
                    "json",
                ],
            )
        if create_res.returncode != 0:
            raise RuntimeError(
                f"Falha ao criar conector {connector_name}: {create_res.stderr.strip()}"
            )
        created_obj = cast(dict[str, Any], json.loads(create_res.stdout))
        raw_id = created_obj.get("id")
        connector_id = (
            str(raw_id) if raw_id is not None else find_connector_id(cli, connector_name, env_id, cluster_id)
        )
        print(f"[ok] Conector criado: {connector_name} ({connector_id})")

    if not connector_id:
        raise RuntimeError("Não foi possível resolver o ID do conector criado.")

    final_status = wait_connector_running(cli, connector_id, env_id, cluster_id)
    print(f"CDC_CONNECTOR_READY: {connector_name} ({connector_id}) -> {final_status}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
