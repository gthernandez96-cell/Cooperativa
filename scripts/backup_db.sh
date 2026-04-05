#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
DB_FILE="$ROOT_DIR/cooperativa.db"
BACKUP_DIR="$ROOT_DIR/backups"

mkdir -p "$BACKUP_DIR"

if [[ ! -f "$DB_FILE" ]]; then
  echo "No existe la base de datos: $DB_FILE" >&2
  exit 1
fi

TS="$(date +%Y%m%d_%H%M%S)"
OUT="$BACKUP_DIR/cooperativa_${TS}.db"

cp "$DB_FILE" "$OUT"

echo "Backup creado: $OUT"

# Retención: conserva 14 backups más recientes
ls -1t "$BACKUP_DIR"/cooperativa_*.db 2>/dev/null | tail -n +15 | xargs -I{} rm -f "{}"
