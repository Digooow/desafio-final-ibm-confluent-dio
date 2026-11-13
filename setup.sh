#!/usr/bin/env bash
set -euo pipefail

# ==============================================================================
# Desafio Final IBM + Confluent (DIO) — Setup Automatizado (Idempotente)
# Camada 1: Fundação (Environment, Cluster Basic, Service Accounts, ACLs e Keys)
# ==============================================================================

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ENV_FILE="$ROOT_DIR/.env"
ENV_EXAMPLE="$ROOT_DIR/.env.example"

# Carrega ferramentas instaladas via WinGet caso o terminal Windows ainda esteja com PATH antigo
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

require_cmd() {
  if ! command -v "$1" >/dev/null 2>&1; then
    echo "ERRO: comando '$1' não encontrado no PATH." >&2
    exit 1
  fi
}

# Atualiza ou insere uma chave no .env local sem interpretar escapes nem imprimir segredos
upsert_env() {
  local key="$1"
  local val="$2"
  local tmp_file="$ENV_FILE.tmp"
  if grep -qE "^${key}=" "$ENV_FILE"; then
    UPSERT_KEY="$key" UPSERT_VAL="$val" awk 'BEGIN{FS="="} $1==ENVIRON["UPSERT_KEY"] {print ENVIRON["UPSERT_KEY"] "=" ENVIRON["UPSERT_VAL"]; next} {print}' "$ENV_FILE" > "$tmp_file"
  else
    cp "$ENV_FILE" "$tmp_file"
    printf "%s=%s\n" "$key" "$val" >> "$tmp_file"
  fi
  mv "$tmp_file" "$ENV_FILE"
}

load_env() {
  if [[ ! -f "$ENV_FILE" ]]; then
    cp "$ENV_EXAMPLE" "$ENV_FILE"
    echo "[INFO] Arquivo .env criado a partir de .env.example (ignorado pelo Git)."
  fi
  local line k v
  while IFS= read -r line || [[ -n "$line" ]]; do
    line="${line%$'\r'}"
    [[ -z "$line" || "$line" == \#* ]] && continue
    k="${line%%=*}"
    v="${line#*=}"
    export "$k=$v"
  done < "$ENV_FILE"
}

resolve_windows_tools
require_cmd confluent
require_cmd jq

# Normaliza CRLF emitido pelo jq.exe no Windows
jq() {
  command jq "$@" | tr -d '\r'
}

load_env

# Verifica sessão ativa na Confluent CLI
if ! confluent environment list >/dev/null 2>&1; then
  echo "ERRO: você não está autenticado na Confluent CLI. Rode 'confluent login' primeiro." >&2
  exit 1
fi

ENV_NAME="${CONFLUENT_ENV_NAME:-desafio-final}"
CLUSTER_NAME="${CONFLUENT_CLUSTER_NAME:-desafio-basic}"
CLOUD_PROVIDER="${CONFLUENT_CLOUD_PROVIDER:-aws}"
CLOUD_REGION="${CONFLUENT_CLOUD_REGION:-us-east-1}"

echo "==> [Camada 1] Verificando Environment '$ENV_NAME'..."
ENV_ID="$(confluent environment list -o json | jq -r --arg name "$ENV_NAME" '.[] | select(.name == $name) | .id' | head -n1)"
if [[ -z "$ENV_ID" ]]; then
  ENV_ID="$(confluent environment create "$ENV_NAME" --governance-package essentials -o json | jq -r '.id')"
  echo "    Environment criado: $ENV_ID (governance: essentials)"
else
  echo "    Environment existente reutilizado: $ENV_ID"
fi
confluent environment use "$ENV_ID" >/dev/null
upsert_env "CONFLUENT_ENV_ID" "$ENV_ID"

echo "==> [Camada 1] Verificando Cluster Basic '$CLUSTER_NAME' ($CLOUD_PROVIDER/$CLOUD_REGION)..."
CLUSTER_ID="$(confluent kafka cluster list --environment "$ENV_ID" -o json | jq -r --arg name "$CLUSTER_NAME" '.[] | select(.name == $name) | .id' | head -n1)"
if [[ -z "$CLUSTER_ID" ]]; then
  CLUSTER_ID="$(confluent kafka cluster create "$CLUSTER_NAME" \
    --cloud "$CLOUD_PROVIDER" \
    --region "$CLOUD_REGION" \
    --type basic \
    --environment "$ENV_ID" \
    -o json | jq -r '.id')"
  echo "    Cluster criado: $CLUSTER_ID"
else
  echo "    Cluster existente reutilizado: $CLUSTER_ID"
fi

echo "==> [Camada 1] Aguardando cluster '$CLUSTER_ID' ficar UP..."
for attempt in $(seq 1 30); do
  CLUSTER_JSON="$(confluent kafka cluster describe "$CLUSTER_ID" --environment "$ENV_ID" -o json)"
  STATUS="$(echo "$CLUSTER_JSON" | jq -r '.status')"
  if [[ "$STATUS" == "UP" ]]; then
    break
  fi
  echo "    Status atual: $STATUS (tentativa $attempt/30)..."
  sleep 5
done

if [[ "${STATUS:-}" != "UP" ]]; then
  echo "ERRO: cluster '$CLUSTER_ID' não ficou UP a tempo (status: ${STATUS:-desconhecido})." >&2
  exit 1
fi

BOOTSTRAP_RAW="$(echo "$CLUSTER_JSON" | jq -r '.endpoint')"
BOOTSTRAP_CLEAN="${BOOTSTRAP_RAW#SASL_SSL://}"
confluent kafka cluster use "$CLUSTER_ID" >/dev/null
upsert_env "CONFLUENT_CLUSTER_ID" "$CLUSTER_ID"
upsert_env "KAFKA_BOOTSTRAP_SERVERS" "$BOOTSTRAP_CLEAN"
echo "    Cluster UP! Endpoint: $BOOTSTRAP_CLEAN"

ensure_sa() {
  local sa_name="$1"
  local sa_desc="$2"
  local sa_id
  sa_id="$(confluent iam service-account list -o json | jq -r --arg name "$sa_name" '.[] | select(.name == $name) | .id' | head -n1)"
  if [[ -z "$sa_id" ]]; then
    sa_id="$(confluent iam service-account create "$sa_name" --description "$sa_desc" -o json | jq -r '.id')"
    echo "    Service Account criada: $sa_name ($sa_id)" >&2
  else
    echo "    Service Account existente reutilizada: $sa_name ($sa_id)" >&2
  fi
  echo "$sa_id"
}

echo "==> [Camada 1] Configurando Service Accounts (menor privilégio)..."
PRODUCER_SA_ID="$(ensure_sa "desafio-producer" "Produtor e CDC com permissao apenas de WRITE em topicos desafio-*")"
CONSUMER_SA_ID="$(ensure_sa "desafio-consumer" "Consumidor com permissao apenas de READ em topicos e grupo desafio-*")"

echo "==> [Camada 1] Aplicando ACLs de menor privilégio..."
# Producer: WRITE + DESCRIBE em prefixos desafio- e desafio. (ex.: desafio.fraud.detected), além de IDEMPOTENT_WRITE no cluster
confluent kafka acl create --allow --service-account "$PRODUCER_SA_ID" --operations write,describe --topic "desafio-" --prefix --cluster "$CLUSTER_ID" --environment "$ENV_ID" >/dev/null
confluent kafka acl create --allow --service-account "$PRODUCER_SA_ID" --operations write,describe --topic "desafio." --prefix --cluster "$CLUSTER_ID" --environment "$ENV_ID" >/dev/null
confluent kafka acl create --allow --service-account "$PRODUCER_SA_ID" --operations idempotent-write --cluster-scope --cluster "$CLUSTER_ID" --environment "$ENV_ID" >/dev/null

# Consumer: READ + DESCRIBE em tópicos desafio-* / desafio.* e consumer group desafio-*
confluent kafka acl create --allow --service-account "$CONSUMER_SA_ID" --operations read,describe --topic "desafio-" --prefix --cluster "$CLUSTER_ID" --environment "$ENV_ID" >/dev/null
confluent kafka acl create --allow --service-account "$CONSUMER_SA_ID" --operations read,describe --topic "desafio." --prefix --cluster "$CLUSTER_ID" --environment "$ENV_ID" >/dev/null
confluent kafka acl create --allow --service-account "$CONSUMER_SA_ID" --operations read,describe --consumer-group "desafio-" --prefix --cluster "$CLUSTER_ID" --environment "$ENV_ID" >/dev/null

echo "==> [Camada 1] Gerando API Keys faltantes (gravadas apenas no .env local)..."
if [[ -z "${PRODUCER_API_KEY:-}" || -z "${PRODUCER_API_SECRET:-}" ]]; then
  P_KEY_JSON="$(confluent api-key create --resource "$CLUSTER_ID" --service-account "$PRODUCER_SA_ID" --environment "$ENV_ID" --description "desafio-producer Kafka key" -o json)"
  upsert_env "PRODUCER_API_KEY" "$(echo "$P_KEY_JSON" | jq -r '.api_key')"
  upsert_env "PRODUCER_API_SECRET" "$(echo "$P_KEY_JSON" | jq -r '.api_secret')"
  echo "    API Key de desafio-producer gerada e salva no .env."
else
  echo "    API Key de desafio-producer já presente no .env (reutilizada)."
fi

if [[ -z "${CONSUMER_API_KEY:-}" || -z "${CONSUMER_API_SECRET:-}" ]]; then
  C_KEY_JSON="$(confluent api-key create --resource "$CLUSTER_ID" --service-account "$CONSUMER_SA_ID" --environment "$ENV_ID" --description "desafio-consumer Kafka key" -o json)"
  upsert_env "CONSUMER_API_KEY" "$(echo "$C_KEY_JSON" | jq -r '.api_key')"
  upsert_env "CONSUMER_API_SECRET" "$(echo "$C_KEY_JSON" | jq -r '.api_secret')"
  echo "    API Key de desafio-consumer gerada e salva no .env."
else
  echo "    API Key de desafio-consumer já presente no .env (reutilizada)."
fi

echo "==> [Camada 1/2] Verificando Schema Registry..."
SR_JSON="$(confluent schema-registry cluster describe --environment "$ENV_ID" -o json)"
SR_ID="$(echo "$SR_JSON" | jq -r '.cluster // .cluster_id // .id // empty')"
SR_URL="$(echo "$SR_JSON" | jq -r '.endpoint_url // .endpoint // empty')"
if [[ -z "$SR_ID" || -z "$SR_URL" ]]; then
  echo "ERRO: não foi possível identificar Schema Registry ID/URL para o environment $ENV_ID." >&2
  exit 1
fi
upsert_env "SCHEMA_REGISTRY_URL" "$SR_URL"

if [[ -z "${SCHEMA_REGISTRY_API_KEY:-}" || -z "${SCHEMA_REGISTRY_API_SECRET:-}" ]]; then
  SR_KEY_JSON="$(confluent api-key create --resource "$SR_ID" --environment "$ENV_ID" --description "desafio Schema Registry key" -o json)"
  upsert_env "SCHEMA_REGISTRY_API_KEY" "$(echo "$SR_KEY_JSON" | jq -r '.api_key')"
  upsert_env "SCHEMA_REGISTRY_API_SECRET" "$(echo "$SR_KEY_JSON" | jq -r '.api_secret')"
  echo "    API Key do Schema Registry gerada e salva no .env."
else
  echo "    API Key do Schema Registry já presente no .env (reutilizada)."
fi

if [[ -z "${METRICS_API_KEY:-}" || -z "${METRICS_API_SECRET:-}" ]]; then
  M_KEY_JSON="$(confluent api-key create --resource cloud --description "desafio Metrics API key" -o json)"
  upsert_env "METRICS_API_KEY" "$(echo "$M_KEY_JSON" | jq -r '.api_key')"
  upsert_env "METRICS_API_SECRET" "$(echo "$M_KEY_JSON" | jq -r '.api_secret')"
  echo "    API Key da Metrics API (cloud) gerada e salva no .env."
else
  echo "    API Key da Metrics API já presente no .env (reutilizada)."
fi

echo ""
echo "==> Executando Camada 2 (Schemas e Compatibilidade)..."
bash "$ROOT_DIR/scripts/register_schemas.sh"

# Resolve interpretador Python do virtualenv local (Windows ou POSIX)
PYTHON_BIN=""
if [[ -x "$ROOT_DIR/.venv/Scripts/python.exe" ]]; then
  PYTHON_BIN="$ROOT_DIR/.venv/Scripts/python.exe"
elif [[ -x "$ROOT_DIR/.venv/bin/python" ]]; then
  PYTHON_BIN="$ROOT_DIR/.venv/bin/python"
elif command -v python3 >/dev/null 2>&1; then
  PYTHON_BIN="python3"
elif command -v python >/dev/null 2>&1; then
  PYTHON_BIN="python"
fi

load_env
if [[ -n "${DATABASE_URL_UNPOOLED:-}" && -n "$PYTHON_BIN" ]]; then
  echo ""
  echo "==> Executando Camada 3 (Banco Neon, Seed idempotente e Conector CDC)..."
  "$PYTHON_BIN" -m scripts.setup_db
  "$PYTHON_BIN" -m scripts.seed_db
  "$PYTHON_BIN" -m scripts.deploy_cdc

  echo ""
  echo "==> Executando Camada 4 (Flink Compute Pool e Job RULE_VELOCITY)..."
  "$PYTHON_BIN" -m scripts.deploy_flink
else
  echo ""
  echo "[INFO] DATABASE_URL_UNPOOLED ou virtualenv Python ainda não configurados; Camadas 3 e 4 serão executadas após vincular o Neon."
fi

echo ""
echo "✅ Setup completo finalizado! Credenciais salvas localmente em .env (não versionado)."
