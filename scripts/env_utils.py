"""Utilitários seguros para leitura e atualização de arquivos `.env` sem expor segredos."""

from __future__ import annotations

from pathlib import Path

from dotenv import dotenv_values


def load_env(env_path: Path | str = ".env") -> dict[str, str]:
    """Carrega pares chave=valor de um arquivo `.env`, ignorando entradas nulas."""
    path = Path(env_path)
    if not path.is_file():
        raise FileNotFoundError(f"Arquivo de ambiente não encontrado: {path}")
    raw = dotenv_values(path)
    return {k: v for k, v in raw.items() if v is not None}


def upsert_env(updates: dict[str, str], env_path: Path | str = ".env") -> None:
    """Atualiza ou insere chaves no `.env` preservando comentários, ordem e outras variáveis."""
    path = Path(env_path)
    lines = path.read_text(encoding="utf-8").splitlines() if path.exists() else []
    pending = dict(updates)
    result: list[str] = []

    for line in lines:
        stripped = line.lstrip()
        if "=" in line and not stripped.startswith("#"):
            key, _ = line.split("=", 1)
            key_clean = key.strip()
            if key_clean in pending:
                result.append(f"{key_clean}={pending.pop(key_clean)}")
                continue
        result.append(line)

    for key, val in pending.items():
        result.append(f"{key}={val}")

    path.write_text("\n".join(result) + "\n", encoding="utf-8")
