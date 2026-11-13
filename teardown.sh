#!/usr/bin/env bash
set -euo pipefail

# ==============================================================================
# Desafio Final IBM + Confluent (DIO) — Teardown Completo
# Remove todos os recursos da nuvem para interromper qualquer cobrança/consumo.
# ==============================================================================

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ENV_FILE="$ROOT_DIR/.env"

resolve_windows_tools() {
  if [[ -n "${LOCALAPPDATA:-}" ]] && [[ -d "$LOCALAPPDATA/Microsoft/WinGet/Packages" ]]; then
    local winget_unix
    winget_unix="$(cygpath -u "$LOCALAPPDATA/Microsoft/WinGet/Packages" 2>/dev/null || echo "")"
    if [[ -n "$winget_unix" && -d "$winget_unix" ]]; then
      for bin_path in \
        "$winget_unix"/ConfluentInc.Confluent-CLI*/confluent \
        "$winget_unix"/jqlang.jq*; do
        if [[ -d "$bin_path" && ":$PATH:" != *":$bin_path:"* ]]; then
          export PATH="$PATH:$bin_path"
        fi
      done
    fi
  fi
}

resolve_windows_tools

if ! command -v confluent >/dev/null 2>&1 || ! command -v jq >/dev/null 2>&1; then
  echo "ERRO: 'confluent' e 'jq' são necessários no PATH." >&2
  exit 1
fi

# Normaliza CRLF emitido pelo jq.exe no Windows para evitar '\r' em IDs capturados no Git Bash
jq() {
  command jq "$@" | tr -d '\r'
}

ENV_NAME="desafio-final"
CLOUD_PROVIDER="aws"
CLOUD_REGION="us-east-1"
if [[ -f "$ENV_FILE" ]]; then
  while IFS= read -r line || [[ -n "$line" ]]; do
    line="${line%$'\r'}"
    [[ -z "$line" || "$line" == \#* ]] && continue
    k="${line%%=*}"
    v="${line#*=}"
    export "$k=$v"
  done < "$ENV_FILE"
  ENV_NAME="${CONFLUENT_ENV_NAME:-desafio-final}"
  CLOUD_PROVIDER="${CONFLUENT_CLOUD_PROVIDER:-aws}"
  CLOUD_REGION="${CONFLUENT_CLOUD_REGION:-us-east-1}"
fi

AUTO_YES="${1:-}"
if [[ "$AUTO_YES" != "--yes" ]]; then
  read -r -p "ATENÇÃO: isso vai APAGAR o environment '$ENV_NAME', clusters, conectores, Flink statements e Service Accounts. Continuar? [y/N] " ans
  if [[ ! "$ans" =~ ^[Yy]$ ]]; then
    echo "Teardown cancelado."
    exit 0
  fi
fi

ENV_ID="$(confluent environment list -o json | jq -r --arg name "$ENV_NAME" '.[] | select(.name == $name) | .id' | head -n1)"

if [[ -n "$ENV_ID" ]]; then
  echo "==> Limpando recursos no environment $ENV_NAME ($ENV_ID)..."
  confluent environment use "$ENV_ID" >/dev/null

  # 1. Encerrar Flink statements e compute pools, se existirem
  while IFS= read -r stmt; do
    [[ -n "$stmt" ]] && confluent flink statement delete "$stmt" --environment "$ENV_ID" --cloud "$CLOUD_PROVIDER" --region "$CLOUD_REGION" --force || true
  done < <(confluent flink statement list --environment "$ENV_ID" --cloud "$CLOUD_PROVIDER" --region "$CLOUD_REGION" -o json 2>/dev/null | jq -r '.[].name // empty' || true)

  echo "--- Verificação Flink statements pós-limpeza:"
  confluent flink statement list --environment "$ENV_ID" --cloud "$CLOUD_PROVIDER" --region "$CLOUD_REGION" || true

  while IFS= read -r pool; do
    [[ -n "$pool" ]] && confluent flink compute-pool delete "$pool" --environment "$ENV_ID" --force || true
  done < <(confluent flink compute-pool list --environment "$ENV_ID" -o json 2>/dev/null | jq -r '.[].id // empty' || true)

  echo "--- Verificação Flink compute pools pós-limpeza:"
  confluent flink compute-pool list --environment "$ENV_ID" || true

  # 2. Remover conectores e clusters Kafka no environment
  while IFS= read -r cid; do
    if [[ -n "$cid" ]]; then
      while IFS= read -r conn; do
        [[ -n "$conn" ]] && confluent connect cluster delete "$conn" --cluster "$cid" --environment "$ENV_ID" --force || true
      done < <(confluent connect cluster list --cluster "$cid" --environment "$ENV_ID" -o json 2>/dev/null | jq -r '.[].id // empty' || true)

      echo "--- Verificação conectores Kafka Connect ($cid) pós-limpeza:"
      confluent connect cluster list --cluster "$cid" --environment "$ENV_ID" || true

      echo "    Deletando cluster Kafka $cid..."
      confluent kafka cluster delete "$cid" --environment "$ENV_ID" --force
    fi
  done < <(confluent kafka cluster list --environment "$ENV_ID" -o json | jq -r '.[].id // empty')

  echo "    Deletando environment $ENV_ID..."
  confluent environment delete "$ENV_ID" --force
else
  echo "[INFO] Environment '$ENV_NAME' já não existe."
fi

# 3. Remover API Keys salvas no .env e Service Accounts do desafio
for key_var in PRODUCER_API_KEY CONSUMER_API_KEY SCHEMA_REGISTRY_API_KEY METRICS_API_KEY; do
  key_val="${!key_var:-}"
  if [[ -n "$key_val" ]]; then
    confluent api-key delete "$key_val" --force >/dev/null 2>&1 || true
  fi
done

for sa_name in desafio-producer desafio-consumer; do
  sa_id="$(confluent iam service-account list -o json | jq -r --arg name "$sa_name" '.[] | select(.name == $name) | .id' | head -n1)"
  if [[ -n "$sa_id" ]]; then
    echo "    Deletando Service Account $sa_name ($sa_id)..."
    confluent iam service-account delete "$sa_id" --force
  fi
done

echo ""
echo "==> Verificação final pós-teardown:"
echo "--- Service Accounts (desafio-*):"
confluent iam service-account list -o json | jq '[.[] | select(.name | startswith("desafio-"))]'
echo "--- Environments (desafio-final):"
confluent environment list -o json | jq '[.[] | select(.name == "desafio-final")]'
echo "--- API Keys restantes (desafio-*):"
confluent api-key list -o json | jq '[.[] | select((.description // "") | startswith("desafio"))]'
echo "✅ Teardown concluído."
