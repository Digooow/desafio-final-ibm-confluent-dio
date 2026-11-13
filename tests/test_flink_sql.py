"""Testes unitários para contratos Flink SQL e utilitários de deploy (Camada 4)."""

from __future__ import annotations

from pathlib import Path

from scripts.deploy_flink import (
    FLINK_PROPERTIES,
    is_benign_alter_error,
    normalize_sql,
    split_sql_statements,
)

ROOT_DIR = Path(__file__).resolve().parent.parent


def test_split_sql_statements_ignores_comments_and_quoted_semicolons() -> None:
    sample = """
    -- comentário com ; falso
    ALTER TABLE `desafio.cdc.public.transactions`
      SET ('note' = 'a;b', 'changelog.mode' = 'append');
    -- outro comentário
    ALTER TABLE `desafio.cdc.public.accounts`
      MODIFY WATERMARK FOR updated_at AS updated_at - INTERVAL '5' SECOND;
    """
    stmts = split_sql_statements(sample)
    assert len(stmts) == 2
    assert "'a;b'" in stmts[0]
    assert stmts[1].startswith("ALTER TABLE `desafio.cdc.public.accounts`")


def test_split_sql_statements_handles_empty_input() -> None:
    assert split_sql_statements("   \n-- only comment\n") == []


def test_sql_scripts_match_challenge_contract() -> None:
    ddl_sql = ROOT_DIR / "sql" / "01_configure_tables.sql"
    fraud_sql = ROOT_DIR / "sql" / "02_fraud_velocity.sql"

    ddl_stmts = split_sql_statements(ddl_sql.read_text(encoding="utf-8"))
    assert len(ddl_stmts) == 4
    ddl_joined = "\n".join(ddl_stmts)
    assert "PRIMARY KEY (transaction_id) NOT ENFORCED" in ddl_joined
    assert "WATERMARK FOR created_at AS created_at - INTERVAL '5' SECOND" in ddl_joined
    assert "'changelog.mode' = 'append'" in ddl_joined
    assert "WATERMARK FOR updated_at AS updated_at - INTERVAL '5' SECOND" in ddl_joined

    fraud_stmts = split_sql_statements(fraud_sql.read_text(encoding="utf-8"))
    assert len(fraud_stmts) == 1
    body = normalize_sql(fraud_stmts[0])
    assert "INSERT INTO `desafio.fraud.detected`" in body
    assert "FOR SYSTEM_TIME AS OF t.created_at" in body
    assert "MATCH_RECOGNIZE" in body
    assert "PARTITION BY card_id" in body
    assert "PATTERN (T{3,}?) WITHIN INTERVAL '60' SECOND" in body
    assert "'RULE_VELOCITY' AS rule_name" in body
    assert "t.status = 'APPROVED'" in body
    assert "a.status = 'ACTIVE'" in body
    assert "sql.tables.scan.idle-timeout=5s" in FLINK_PROPERTIES


def test_is_benign_alter_error_matches_only_idempotency_markers() -> None:
    assert is_benign_alter_error("Primary key is already defined on table")
    assert is_benign_alter_error(
        "The current table has already defined the primary key constraint [`transaction_id`]."
    )
    assert is_benign_alter_error("Constraint pk_transactions ALREADY EXISTS")
    assert not is_benign_alter_error("Syntax error at line 1")


def test_normalize_sql_canonicalizes_whitespace_and_comments() -> None:
    a = "-- header\nSELECT  a,\n  b  FROM tbl;\n"
    b = "SELECT a, b FROM tbl"
    assert normalize_sql(a) == normalize_sql(b)
