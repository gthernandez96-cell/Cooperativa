#!/usr/bin/env bash
set -euo pipefail

if [[ $# -lt 1 ]]; then
  echo "Uso: $0 <archivo_backup.db>" >&2
  exit 1
fi

BACKUP_FILE="$1"
ROOT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
DB_FILE="$ROOT_DIR/cooperativa.db"

if [[ ! -f "$BACKUP_FILE" ]]; then
  echo "No existe el backup: $BACKUP_FILE" >&2
  exit 1
fi

cp "$BACKUP_FILE" "$DB_FILE"
echo "Base restaurada desde: $BACKUP_FILE"
