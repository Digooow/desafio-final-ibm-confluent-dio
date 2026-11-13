"""Provisiona Flink Compute Pool, configura tabelas CDC e implanta o job RULE_VELOCITY."""

from __future__ import annotations

import argparse
import json
import sys
import time
import uuid
from pathlib import Path
from typing import Any, cast

ROOT_DIR = Path(__file__).resolve().parent.parent
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from scripts.confluent_utils import resolve_confluent_bin, run_confluent
from scripts.env_utils import load_env, upsert_env

DEFAULT_POOL_NAME = "desafio-flink"
DEFAULT_MAX_CFU = 5
STATEMENT_NAME = "desafio-fraud-velocity"
FLINK_PROPERTIES: tuple[str, ...] = ("sql.tables.scan.idle-timeout=5s",)


def split_sql_statements(sql_text: str) -> list[str]:
    """Divide script SQL em statements individuais ignorando comentários e ';' dentro de aspas."""
    statements: list[str] = []
    buf: list[str] = []
    in_single = False
    in_backtick = False
    i = 0
    n = len(sql_text)

    while i < n:
        ch = sql_text[i]
        nxt = sql_text[i + 1] if i + 1 < n else ""

        if not in_single and not in_backtick and ch == "-" and nxt == "-":
            while i < n and sql_text[i] != "\n":
                i += 1
            continue

        if ch == "'" and not in_backtick:
            if in_single and nxt == "'":
                buf.append("''")
                i += 2
                continue
            in_single = not in_single
        elif ch == "`" and not in_single:
            in_backtick = not in_backtick
        elif ch == ";" and not in_single and not in_backtick:
            stmt = "".join(buf).strip()
            if stmt:
                statements.append(stmt)
            buf.clear()
            i += 1
            continue

        buf.append(ch)
        i += 1

    trailing = "".join(buf).strip()
    if trailing:
        statements.append(trailing)
    return statements


def normalize_sql(sql: str) -> str:
    """Normaliza espaços em branco e ponto-e-vírgula final para comparação idempotente."""
    cleaned = "\n".join(
        line for line in sql.splitlines() if not line.lstrip().startswith("--")
    )
    return " ".join(cleaned.rstrip().rstrip(";").split())


def is_benign_alter_error(stderr_text: str) -> bool:
    """Identifica erros benignos de idempotência ao reaplicar constraints/watermarks já presentes."""
    lower = stderr_text.lower()
    benign_markers = (
        "already exists",
        "already defined the primary key constraint",
        "primary key is already defined",
        "watermark is already defined",
        "already defined a watermark",
        "already has a watermark",
    )
    return any(marker in lower for marker in benign_markers)


def _find_compute_pool(cli: str, env_id: str, pool_name: str) -> dict[str, Any] | None:
    res = run_confluent(cli, ["flink", "compute-pool", "list", "--environment", env_id, "-o", "json"])
    if res.returncode != 0:
        raise RuntimeError(f"Falha ao listar compute pools Flink: {res.stderr.strip()}")
    pools = cast(list[dict[str, Any]], json.loads(res.stdout or "[]") or [])
    for pool in pools:
        if pool.get("name") == pool_name:
            return pool
    return None


def ensure_compute_pool(
    cli: str,
    env_id: str,
    pool_name: str,
    cloud: str,
    region: str,
    max_cfu: int = DEFAULT_MAX_CFU,
    max_attempts: int = 24,
    interval_sec: int = 5,
) -> str:
    """Garante compute pool Flink ativo no estado PROVISIONED e retorna seu ID."""
    existing = _find_compute_pool(cli, env_id, pool_name)
    if existing:
        pool_id = existing["id"]
        print(f"[ok] Compute pool existente: {pool_name} ({pool_id}, status={existing.get('status')})")
    else:
        create_res = run_confluent(
            cli,
            [
                "flink",
                "compute-pool",
                "create",
                pool_name,
                "--cloud",
                cloud,
                "--region",
                region,
                "--max-cfu",
                str(max_cfu),
                "--environment",
                env_id,
                "-o",
                "json",
            ],
        )
        if create_res.returncode != 0:
            raise RuntimeError(f"Falha ao criar compute pool {pool_name}: {create_res.stderr.strip()}")
        pool_id = json.loads(create_res.stdout)["id"]
        print(f"[ok] Compute pool criado: {pool_name} ({pool_id})")

    for attempt in range(1, max_attempts + 1):
        desc = run_confluent(
            cli,
            ["flink", "compute-pool", "describe", pool_id, "--environment", env_id, "-o", "json"],
        )
        if desc.returncode != 0:
            raise RuntimeError(f"Erro ao descrever compute pool {pool_id}: {desc.stderr.strip()}")
        info = json.loads(desc.stdout)
        status = str(info.get("status", "UNKNOWN")).upper()
        if status == "PROVISIONED":
            return pool_id
        if status in {"FAILED", "DEGRADED"}:
            raise RuntimeError(f"Compute pool {pool_id} em erro: {desc.stdout}")
        print(f"[{attempt}/{max_attempts}] Compute pool {pool_id} status={status}...")
        if attempt < max_attempts:
            time.sleep(interval_sec)

    raise TimeoutError(f"Timeout aguardando compute pool {pool_id} atingir PROVISIONED.")


def apply_table_configurations(
    cli: str,
    env_id: str,
    pool_id: str,
    database: str,
    cloud: str,
    region: str,
    sql_path: Path,
) -> list[str]:
    """Aplica declarações ALTER TABLE em sql/01_configure_tables.sql de forma idempotente."""
    statements = split_sql_statements(sql_path.read_text(encoding="utf-8"))
    results: list[str] = []
    suffix = uuid.uuid4().hex[:6]

    for idx, stmt in enumerate(statements, start=1):
        temp_name = f"desafio-ddl-{idx}-{suffix}"
        res = run_confluent(
            cli,
            [
                "flink",
                "statement",
                "create",
                temp_name,
                "--environment",
                env_id,
                "--compute-pool",
                pool_id,
                "--database",
                database,
                "--sql",
                stmt,
                "--wait",
                "-o",
                "json",
            ],
        )
        # Sempre limpa statement temporário de DDL para manter o ambiente organizado
        run_confluent(
            cli,
            [
                "flink",
                "statement",
                "delete",
                temp_name,
                "--environment",
                env_id,
                "--cloud",
                cloud,
                "--region",
                region,
                "--force",
            ],
        )

        combined_err = f"{res.stderr}\n{res.stdout}".strip()
        if res.returncode != 0:
            if is_benign_alter_error(combined_err):
                print(f"[ok] DDL #{idx} já estava aplicado ({stmt.splitlines()[0]})")
                results.append("ALREADY_APPLIED")
                continue
            raise RuntimeError(f"Falha no DDL #{idx}: {combined_err}")

        payload = cast(
            dict[str, Any],
            json.loads(res.stdout) if res.stdout.strip().startswith("{") else {},
        )
        status = str(payload.get("status", "COMPLETED")).upper()
        if status == "FAILED":
            detail = str(payload.get("status_detail", res.stdout))
            if is_benign_alter_error(detail):
                print(f"[ok] DDL #{idx} já estava aplicado ({stmt.splitlines()[0]})")
                results.append("ALREADY_APPLIED")
                continue
            raise RuntimeError(f"DDL #{idx} falhou: {detail}")

        print(f"[ok] DDL #{idx} concluído -> {status}")
        results.append(status)
    return results


def _find_statement(
    cli: str,
    env_id: str,
    cloud: str,
    region: str,
    name: str,
) -> dict[str, Any] | None:
    res = run_confluent(
        cli,
        ["flink", "statement", "list", "--environment", env_id, "--cloud", cloud, "--region", region, "-o", "json"],
    )
    if res.returncode != 0:
        raise RuntimeError(f"Falha ao listar statements Flink: {res.stderr.strip()}")
    items = cast(list[dict[str, Any]], json.loads(res.stdout or "[]") or [])
    for item in items:
        if item.get("name") == name:
            return item
    return None


def deploy_streaming_statement(
    cli: str,
    env_id: str,
    pool_id: str,
    database: str,
    cloud: str,
    region: str,
    statement_name: str,
    sql_path: Path,
    properties: tuple[str, ...] = FLINK_PROPERTIES,
    recreate: bool = False,
    max_attempts: int = 24,
    interval_sec: int = 5,
) -> dict[str, Any]:
    """Implanta ou reutiliza statement streaming contínuo e valida ausência de exceções."""
    statements = split_sql_statements(sql_path.read_text(encoding="utf-8"))
    if len(statements) != 1:
        raise ValueError(f"{sql_path} deve conter exatamente 1 statement INSERT INTO; encontrados={len(statements)}")
    sql = statements[0]

    existing = _find_statement(cli, env_id, cloud, region, statement_name)
    if existing:
        existing_status = str(existing.get("status", "")).upper()
        same_sql = normalize_sql(str(existing.get("statement", ""))) == normalize_sql(sql)
        if existing_status == "RUNNING" and same_sql and not recreate:
            print(f"[ok] Statement '{statement_name}' já está RUNNING com o SQL atual.")
            return existing
        print(f"[info] Recriando statement '{statement_name}' (status anterior={existing_status}, recreate={recreate})...")
        del_res = run_confluent(
            cli,
            [
                "flink",
                "statement",
                "delete",
                statement_name,
                "--environment",
                env_id,
                "--cloud",
                cloud,
                "--region",
                region,
                "--force",
            ],
        )
        if del_res.returncode != 0:
            raise RuntimeError(f"Falha ao remover statement anterior '{statement_name}': {del_res.stderr.strip()}")

    args = [
        "flink",
        "statement",
        "create",
        statement_name,
        "--environment",
        env_id,
        "--compute-pool",
        pool_id,
        "--database",
        database,
        "--sql",
        sql,
        "-o",
        "json",
    ]
    for prop in properties:
        args.extend(["--property", prop])

    create_res = run_confluent(cli, args)
    if create_res.returncode != 0:
        raise RuntimeError(f"Falha ao submeter statement '{statement_name}': {create_res.stderr.strip()}")

    for attempt in range(1, max_attempts + 1):
        desc_res = run_confluent(
            cli,
            [
                "flink",
                "statement",
                "describe",
                statement_name,
                "--environment",
                env_id,
                "--cloud",
                cloud,
                "--region",
                region,
                "-o",
                "json",
            ],
        )
        if desc_res.returncode != 0:
            raise RuntimeError(f"Erro ao descrever statement '{statement_name}': {desc_res.stderr.strip()}")
        info = cast(dict[str, Any], json.loads(desc_res.stdout))
        status = str(info.get("status", "UNKNOWN")).upper()
        print(f"[{attempt}/{max_attempts}] Statement '{statement_name}' status={status}")
        if status == "RUNNING":
            exc_res = run_confluent(
                cli,
                [
                    "flink",
                    "statement",
                    "exception",
                    "list",
                    statement_name,
                    "--environment",
                    env_id,
                    "--cloud",
                    cloud,
                    "--region",
                    region,
                    "-o",
                    "json",
                ],
            )
            exceptions = (
                cast(list[Any], json.loads(exc_res.stdout or "[]")) if exc_res.returncode == 0 else []
            )
            if exceptions:
                raise RuntimeError(f"Statement RUNNING com exceções registradas: {exceptions}")
            return info
        if status in {"FAILED", "STOPPED"}:
            exc_res = run_confluent(
                cli,
                [
                    "flink",
                    "statement",
                    "exception",
                    "list",
                    statement_name,
                    "--environment",
                    env_id,
                    "--cloud",
                    cloud,
                    "--region",
                    region,
                    "-o",
                    "json",
                ],
            )
            raise RuntimeError(
                f"Statement '{statement_name}' entrou em {status}: {info.get('status_detail')} | exceptions={exc_res.stdout.strip()}"
            )
        if attempt < max_attempts:
            time.sleep(interval_sec)

    raise TimeoutError(f"Timeout aguardando statement '{statement_name}' atingir RUNNING.")


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Implanta Flink SQL da Camada 4 na Confluent Cloud.")
    parser.add_argument("--recreate", action="store_true", help="Recria o statement mesmo se já estiver RUNNING.")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    env = load_env(ROOT_DIR / ".env")
    env_id = env["CONFLUENT_ENV_ID"].strip()
    cluster_id = env["CONFLUENT_CLUSTER_ID"].strip()
    cloud = env.get("CONFLUENT_CLOUD_PROVIDER", "aws").strip()
    region = env.get("CONFLUENT_CLOUD_REGION", "us-east-1").strip()
    pool_name = env.get("FLINK_COMPUTE_POOL_NAME", DEFAULT_POOL_NAME).strip() or DEFAULT_POOL_NAME

    cli = resolve_confluent_bin()
    pool_id = ensure_compute_pool(cli, env_id, pool_name, cloud, region)
    upsert_env({"FLINK_COMPUTE_POOL_ID": pool_id, "FLINK_COMPUTE_POOL_NAME": pool_name}, ROOT_DIR / ".env")

    apply_table_configurations(
        cli=cli,
        env_id=env_id,
        pool_id=pool_id,
        database=cluster_id,
        cloud=cloud,
        region=region,
        sql_path=ROOT_DIR / "sql" / "01_configure_tables.sql",
    )

    info = deploy_streaming_statement(
        cli=cli,
        env_id=env_id,
        pool_id=pool_id,
        database=cluster_id,
        cloud=cloud,
        region=region,
        statement_name=STATEMENT_NAME,
        sql_path=ROOT_DIR / "sql" / "02_fraud_velocity.sql",
        recreate=args.recreate,
    )
    print(f"FLINK_JOB_READY: {info.get('name')} ({pool_id}) -> {info.get('status')}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
