#!/usr/bin/env bash
set -euo pipefail

APP_DIR="/home/gthernandez96/cooperativa"
WSGI_FILE="/var/www/gthernandez96_pythonanywhere_com_wsgi.py"
SKIP_GIT_PULL="0"

if [[ ! -d "$APP_DIR" ]]; then
  echo "No existe APP_DIR: $APP_DIR" >&2
  echo "Sugerencia: find /home/gthernandez96 -maxdepth 5 -name app.py -type f" >&2
  exit 1
fi

cd "$APP_DIR"

echo "[1/5] Respaldo remoto..."
[[ -f cooperativa.db ]] && cp cooperativa.db "cooperativa.db.bak.$(date +%F_%H%M%S)"
[[ -d static/uploads ]] && tar -czf "uploads.bak.$(date +%F_%H%M%S).tar.gz" static/uploads

if [[ "$SKIP_GIT_PULL" != "1" ]] && [[ -d .git ]]; then
  echo "[2/5] git pull..."
  git pull --ff-only || true
else
  echo "[2/5] git pull omitido"
fi

echo "[3/5] Aplicando base SQLite..."
cp "/home/gthernandez96/cooperativa.db" "$APP_DIR/cooperativa.db"

echo "[4/5] Aplicando uploads..."
tar -xzf "/home/gthernandez96/uploads_local.tar.gz" -C "$APP_DIR"

echo "[5/5] Verificando y recargando..."
sqlite3 "$APP_DIR/cooperativa.db" "SELECT COUNT(*) AS total_socios FROM socios;"
touch "$WSGI_FILE"

echo "Sincronizacion completada."
