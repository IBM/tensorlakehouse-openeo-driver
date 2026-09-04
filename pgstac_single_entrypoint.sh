#!/usr/bin/env bash
# Entrypoint for the fused pgstac + stac-fastapi-pgstac container.
# Starts PostgreSQL (PostGIS + pgstac) in the background, waits for it to be
# ready, then runs the STAC FastAPI app in the foreground.
set -Eeo pipefail

# The STAC FastAPI app reads its DB connection from these env vars. In the
# single-container setup the database lives on localhost, so default the hosts
# accordingly while still allowing overrides.
export POSTGRES_HOST="${POSTGRES_HOST:-127.0.0.1}"
export POSTGRES_HOST_READER="${POSTGRES_HOST_READER:-127.0.0.1}"
export POSTGRES_HOST_WRITER="${POSTGRES_HOST_WRITER:-127.0.0.1}"
export POSTGRES_PORT="${POSTGRES_PORT:-5432}"
export POSTGRES_PASS="${POSTGRES_PASS:-$POSTGRES_PASSWORD}"
export POSTGRES_DBNAME="${POSTGRES_DBNAME:-$POSTGRES_DB}"

APP_HOST="${APP_HOST:-0.0.0.0}"
APP_PORT="${APP_PORT:-8080}"

# Start PostgreSQL via the stock postgres entrypoint. On first boot (empty
# PGDATA) this also initialises the cluster and runs the pgstac init scripts
# that load PostGIS and the pgstac schema.
#
# The pgstac image's init script sizes shared_buffers/etc. from /proc/meminfo,
# which in a container reports the *host node's* RAM (e.g. 64 GB) rather than
# the container's memory limit. That produces settings (e.g. 16 GB
# shared_buffers) that make postgres fail to start inside a small container.
# We pass conservative, container-appropriate values on the command line,
# which override the ALTER SYSTEM values persisted by the init script.
# dynamic_shared_memory_type=mmap avoids depending on /dev/shm, which is only
# 64 MB in Code Engine.
PG_SHARED_BUFFERS="${PG_SHARED_BUFFERS:-256MB}"
PG_EFFECTIVE_CACHE_SIZE="${PG_EFFECTIVE_CACHE_SIZE:-1GB}"
PG_MAINTENANCE_WORK_MEM="${PG_MAINTENANCE_WORK_MEM:-128MB}"
PG_WORK_MEM="${PG_WORK_MEM:-8MB}"

# Process structure note:
#   PostgreSQL runs in the FOREGROUND as the container's main process, and the
#   STAC FastAPI app runs in a background waiter that launches once the DB is
#   ready. This is deliberate: on Code Engine, backgrounding postgres with `&`
#   and polling caused the postmaster to die before emitting any log line,
#   while running postgres in the foreground (exec) starts cleanly and reaches
#   "ready to accept connections". So we keep postgres in the proven-good
#   foreground path and make the API the background child instead.
#
# The background waiter blocks on pg_isready, then exec's uvicorn. Because it
# is a child of this shell (which we exec into postgres below), it keeps
# running after the exec and is reparented to the postgres process (same PID).
echo "[pgstac-single] launching API waiter (starts once PostgreSQL is ready)..."
(
  until pg_isready -h 127.0.0.1 -p 5432 -U "$POSTGRES_USER" -d "$POSTGRES_DB" >/dev/null 2>&1; do
    sleep 1
  done
  echo "[pgstac-single] PostgreSQL is ready; starting stac-fastapi-pgstac on ${APP_HOST}:${APP_PORT}..."
  exec uvicorn stac_fastapi.pgstac.app:app --host "$APP_HOST" --port "$APP_PORT"
) &

# Start PostgreSQL in the foreground as PID-preserving main process. If it
# exits, the container exits and Code Engine restarts it.
echo "[pgstac-single] starting PostgreSQL (shared_buffers=${PG_SHARED_BUFFERS})..."
exec docker-entrypoint.sh postgres \
  -c shared_buffers="${PG_SHARED_BUFFERS}" \
  -c effective_cache_size="${PG_EFFECTIVE_CACHE_SIZE}" \
  -c maintenance_work_mem="${PG_MAINTENANCE_WORK_MEM}" \
  -c work_mem="${PG_WORK_MEM}" \
  -c dynamic_shared_memory_type=mmap
