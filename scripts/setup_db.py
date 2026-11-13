"""Configura schema DDL, REPLICA IDENTITY FULL, publicação Debezium e role `cdc_user` no Neon."""

from __future__ import annotations

import secrets
import string
import sys
from pathlib import Path
from typing import LiteralString, cast

import psycopg
from psycopg import sql

ROOT_DIR = Path(__file__).resolve().parent.parent
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from scripts.env_utils import load_env, upsert_env

TABLES = ("customers", "accounts", "cards", "merchants", "transactions")
PUBLICATION_NAME = "dbz_publication"


def generate_safe_password(length: int = 32) -> str:
    """Gera senha aleatória forte sem caracteres problemáticos para URL/JSON/Shell."""
    alphabet = string.ascii_letters + string.digits
    return "".join(secrets.choice(alphabet) for _ in range(length))


def apply_schema(conn: psycopg.Connection, schema_path: Path) -> None:
    """Executa o arquivo DDL no banco usando conexão direta (não-pooled)."""
    ddl_sql = schema_path.read_text(encoding="utf-8")
    with conn.cursor() as cur:
        cur.execute(cast(LiteralString, ddl_sql))
    conn.commit()


def ensure_cdc_role(
    conn: psycopg.Connection,
    database: str,
    cdc_user: str,
    cdc_password: str,
) -> None:
    """Cria ou atualiza a role de CDC com privilégios estritamente mínimos (LOGIN + REPLICATION + SELECT)."""
    with conn.cursor() as cur:
        cur.execute("SELECT 1 FROM pg_roles WHERE rolname = %s;", (cdc_user,))
        exists = cur.fetchone() is not None

        if exists:
            cur.execute(
                sql.SQL(
                    "ALTER ROLE {} WITH LOGIN REPLICATION NOSUPERUSER NOCREATEDB NOCREATEROLE NOBYPASSRLS PASSWORD {};"
                ).format(sql.Identifier(cdc_user), sql.Literal(cdc_password))
            )
        else:
            cur.execute(
                sql.SQL(
                    "CREATE ROLE {} WITH LOGIN REPLICATION NOSUPERUSER NOCREATEDB NOCREATEROLE NOBYPASSRLS PASSWORD {};"
                ).format(sql.Identifier(cdc_user), sql.Literal(cdc_password))
            )

        cur.execute(
            sql.SQL("REVOKE ALL ON SCHEMA public FROM {};").format(sql.Identifier(cdc_user))
        )
        cur.execute(
            sql.SQL("REVOKE ALL ON ALL TABLES IN SCHEMA public FROM {};").format(
                sql.Identifier(cdc_user)
            )
        )
        cur.execute(
            sql.SQL("GRANT CONNECT ON DATABASE {} TO {};").format(
                sql.Identifier(database), sql.Identifier(cdc_user)
            )
        )
        cur.execute(
            sql.SQL("GRANT USAGE ON SCHEMA public TO {};").format(
                sql.Identifier(cdc_user)
            )
        )
        for table in TABLES:
            cur.execute(
                sql.SQL("GRANT SELECT ON TABLE public.{} TO {};").format(
                    sql.Identifier(table), sql.Identifier(cdc_user)
                )
            )
    conn.commit()


def verify_cdc_readiness(
    admin_conn: psycopg.Connection,
    host: str,
    port: int,
    database: str,
    sslmode: str,
    cdc_user: str,
    cdc_password: str,
) -> None:
    """Valida wal_level, REPLICA IDENTITY FULL, publicação e princípio do menor privilégio."""
    with admin_conn.cursor() as cur:
        cur.execute("SHOW wal_level;")
        wal_row = cur.fetchone()
        if wal_row is None:
            raise RuntimeError("SHOW wal_level não retornou resultado.")
        wal_level = str(wal_row[0])
        if wal_level != "logical":
            raise RuntimeError(f"Esperado wal_level=logical, obtido {wal_level!r}")

        cur.execute(
            """
            SELECT c.relname, c.relreplident
            FROM pg_class c
            JOIN pg_namespace n ON n.oid = c.relnamespace
            WHERE n.nspname = 'public' AND c.relname = ANY(%s)
            ORDER BY c.relname;
            """,
            (list(TABLES),),
        )
        repl_rows = dict(cur.fetchall())
        for table in TABLES:
            mode = repl_rows.get(table)
            if mode != "f":
                raise RuntimeError(
                    f"Tabela public.{table} com relreplident={mode!r}; esperado 'f' (FULL)"
                )

        cur.execute(
            """
            SELECT tablename
            FROM pg_publication_tables
            WHERE pubname = %s AND schemaname = 'public'
            ORDER BY tablename;
            """,
            (PUBLICATION_NAME,),
        )
        pub_tables = sorted(row[0] for row in cur.fetchall())
        if pub_tables != sorted(TABLES):
            raise RuntimeError(
                f"Publicação {PUBLICATION_NAME} incompleta: {pub_tables!r}"
            )

        cur.execute(
            """
            SELECT rolsuper, rolcreaterole, rolcreatedb, rolcanlogin, rolreplication
            FROM pg_roles
            WHERE rolname = %s;
            """,
            (cdc_user,),
        )
        attrs = cur.fetchone()
        if attrs != (False, False, False, True, True):
            raise RuntimeError(f"Atributos inesperados para {cdc_user}: {attrs!r}")

    # Verifica privilégios reais conectando como cdc_user
    with (
        psycopg.connect(
            host=host,
            port=port,
            dbname=database,
            user=cdc_user,
            password=cdc_password,
            sslmode=sslmode,
            connect_timeout=15,
        ) as cdc_conn,
        cdc_conn.cursor() as cur,
    ):
        cur.execute("SELECT COUNT(*) FROM public.transactions;")
        cur.fetchone()
        try:
            cur.execute(
                """
                INSERT INTO public.customers (full_name, email, document_id)
                VALUES ('Probe', 'probe@example.test', '***-000000-**');
                """
            )
        except psycopg.errors.InsufficientPrivilege:
            cdc_conn.rollback()
        else:
            cdc_conn.rollback()
            raise RuntimeError(
                f"VIOLAÇÃO DE MENOR PRIVILÉGIO: {cdc_user} conseguiu inserir em public.customers"
            )


def main() -> int:
    env_path = ROOT_DIR / ".env"
    env = load_env(env_path)

    unpooled_url = env.get("DATABASE_URL_UNPOOLED", "").strip()
    if not unpooled_url or "-pooler" in env.get("PG_HOST", ""):
        raise SystemExit(
            "ERRO: DATABASE_URL_UNPOOLED ausente ou PG_HOST usando pooler; CDC exige conexão direta."
        )

    cdc_user = env.get("PG_CDC_USER", "cdc_user").strip() or "cdc_user"
    cdc_password = env.get("PG_CDC_PASSWORD", "").strip()
    if not cdc_password:
        cdc_password = generate_safe_password()
        upsert_env({"PG_CDC_USER": cdc_user, "PG_CDC_PASSWORD": cdc_password}, env_path)

    schema_path = ROOT_DIR / "db" / "01_schema.sql"
    database = env.get("PG_DATABASE", "neondb").strip() or "neondb"
    host = env["PG_HOST"].strip()
    port = int(env.get("PG_PORT", "5432").strip() or "5432")
    sslmode = env.get("PG_SSLMODE", "require").strip() or "require"

    with psycopg.connect(unpooled_url, connect_timeout=20) as conn:
        apply_schema(conn, schema_path)
        ensure_cdc_role(conn, database, cdc_user, cdc_password)
        verify_cdc_readiness(conn, host, port, database, sslmode, cdc_user, cdc_password)

    print("DB_SETUP_OK: schema, REPLICA IDENTITY FULL, publicacao dbz_publication e cdc_user (menor privilegio) validados.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
