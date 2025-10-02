#!/usr/bin/env sh
set -eu

# Porta interna su cui il nodo ascolta
PORT="${PORT:-9002}"

# Indirizzo di bind (sempre all'interno del container)
BIND_ADDR="0.0.0.0:${PORT}"

# Hostname univoco del container (es. gossipsystutil-peer-1)
HOSTNAME_IN_CONTAINER="$(hostname)"

# Indirizzo da ANNUNCIARE agli altri nodi sulla rete docker
ADVERTISE_ADDR="${HOSTNAME_IN_CONTAINER}:${PORT}"

# Se l’utente ha passato GRPC_ADDR esplicito (semi), rispettalo
if [ "${GRPC_ADDR:-}" = "" ]; then
  export GRPC_ADDR="${ADVERTISE_ADDR}"
fi

# Alcuni progetti distinguono bind vs advertise.
# Se il tuo binario supporta BIND_ADDR, glielo passiamo:
export BIND_ADDR="${BIND_ADDR}"

# Log utili in chiaro
echo "[entrypoint] PORT=${PORT}"
echo "[entrypoint] BIND_ADDR=${BIND_ADDR}"
echo "[entrypoint] GRPC_ADDR=${GRPC_ADDR}"
echo "[entrypoint] SEEDS=${SEEDS:-<none>}"
echo "[entrypoint] IS_SEED=${IS_SEED:-false}"

exec /app/node
