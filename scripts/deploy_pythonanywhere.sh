#!/usr/bin/env bash
set -euo pipefail

# Sincroniza SQLite + uploads hacia PythonAnywhere.
# Puede operar en modo completo (ssh/scp) o solo generar paquete para subida manual.

ROOT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
LOCAL_DB="$ROOT_DIR/cooperativa.db"
LOCAL_UPLOADS_DIR="$ROOT_DIR/static/uploads"
OUT_DIR="$ROOT_DIR/dist/pythonanywhere_sync"

PA_USER=""
PA_HOST=""
PA_APP_DIR=""
PA_WSGI=""
BUNDLE_ONLY=0
SKIP_GIT_PULL=0

usage() {
  cat <<'EOF'
Uso:
  scripts/deploy_pythonanywhere.sh --user <usuario>
  scripts/deploy_pythonanywhere.sh --user <usuario> --app-dir /home/<usuario>/<proyecto>
  scripts/deploy_pythonanywhere.sh --user <usuario> --bundle-only

Opciones:
  --user <usuario>         Usuario PythonAnywhere (ej: gthernandez96)
  --app-dir <ruta>         Ruta del proyecto en PythonAnywhere (opcional)
  --wsgi-file <ruta>       WSGI path (default: /var/www/<usuario>_pythonanywhere_com_wsgi.py)
  --bundle-only            Solo genera paquete para subida manual por Files
  --skip-git-pull          No ejecutar git pull en servidor
  -h, --help               Mostrar ayuda

Salida generada:
  dist/pythonanywhere_sync/cooperativa.db
  dist/pythonanywhere_sync/uploads_local.tar.gz
  dist/pythonanywhere_sync/remote_apply.sh
EOF
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --user)
      PA_USER="${2:-}"
      shift 2
      ;;
    --app-dir)
      PA_APP_DIR="${2:-}"
      shift 2
      ;;
    --wsgi-file)
      PA_WSGI="${2:-}"
      shift 2
      ;;
    --bundle-only)
      BUNDLE_ONLY=1
      shift
      ;;
    --skip-git-pull)
      SKIP_GIT_PULL=1
      shift
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      echo "Parametro desconocido: $1" >&2
      usage
      exit 1
      ;;
  esac
done

if [[ -z "$PA_USER" ]]; then
  echo "Debes indicar --user <usuario>." >&2
  usage
  exit 1
fi

if [[ -z "$PA_APP_DIR" ]]; then
  PA_APP_DIR="/home/$PA_USER/cooperativa"
fi

if [[ -z "$PA_WSGI" ]]; then
  PA_WSGI="/var/www/${PA_USER}_pythonanywhere_com_wsgi.py"
fi

PA_HOST="$PA_USER@ssh.pythonanywhere.com"

if [[ ! -f "$LOCAL_DB" ]]; then
  echo "No existe DB local: $LOCAL_DB" >&2
  exit 1
fi

if [[ ! -d "$LOCAL_UPLOADS_DIR" ]]; then
  echo "No existe carpeta uploads local: $LOCAL_UPLOADS_DIR" >&2
  exit 1
fi

mkdir -p "$OUT_DIR"
cp "$LOCAL_DB" "$OUT_DIR/cooperativa.db"
tar -czf "$OUT_DIR/uploads_local.tar.gz" -C "$ROOT_DIR" static/uploads

cat > "$OUT_DIR/remote_apply.sh" <<EOF
#!/usr/bin/env bash
set -euo pipefail

APP_DIR="$PA_APP_DIR"
WSGI_FILE="$PA_WSGI"
SKIP_GIT_PULL="$SKIP_GIT_PULL"

if [[ ! -d "\$APP_DIR" ]]; then
  echo "No existe APP_DIR: \$APP_DIR" >&2
  echo "Sugerencia: find /home/$PA_USER -maxdepth 5 -name app.py -type f" >&2
  exit 1
fi

cd "\$APP_DIR"

echo "[1/5] Respaldo remoto..."
[[ -f cooperativa.db ]] && cp cooperativa.db "cooperativa.db.bak.\$(date +%F_%H%M%S)"
[[ -d static/uploads ]] && tar -czf "uploads.bak.\$(date +%F_%H%M%S).tar.gz" static/uploads

if [[ "\$SKIP_GIT_PULL" != "1" ]] && [[ -d .git ]]; then
  echo "[2/5] git pull..."
  git pull --ff-only || true
else
  echo "[2/5] git pull omitido"
fi

echo "[3/5] Aplicando base SQLite..."
cp "/home/$PA_USER/cooperativa.db" "\$APP_DIR/cooperativa.db"

echo "[4/5] Aplicando uploads..."
tar -xzf "/home/$PA_USER/uploads_local.tar.gz" -C "\$APP_DIR"

echo "[5/5] Verificando y recargando..."
sqlite3 "\$APP_DIR/cooperativa.db" "SELECT COUNT(*) AS total_socios FROM socios;"
touch "\$WSGI_FILE"

echo "Sincronizacion completada."
EOF

chmod +x "$OUT_DIR/remote_apply.sh"

echo "Paquete generado en: $OUT_DIR"
echo "- $OUT_DIR/cooperativa.db"
echo "- $OUT_DIR/uploads_local.tar.gz"
echo "- $OUT_DIR/remote_apply.sh"

if [[ "$BUNDLE_ONLY" == "1" ]]; then
  cat <<EOF

Modo bundle-only:
1. Sube cooperativa.db, uploads_local.tar.gz y remote_apply.sh a /home/$PA_USER en PythonAnywhere (pestana Files).
2. En consola Bash de PythonAnywhere ejecuta:
   bash /home/$PA_USER/remote_apply.sh
EOF
  exit 0
fi

echo "Iniciando despliegue via ssh/scp a $PA_HOST ..."

scp "$OUT_DIR/cooperativa.db" "$PA_HOST:/home/$PA_USER/"
scp "$OUT_DIR/uploads_local.tar.gz" "$PA_HOST:/home/$PA_USER/"
scp "$OUT_DIR/remote_apply.sh" "$PA_HOST:/home/$PA_USER/"

ssh "$PA_HOST" "bash /home/$PA_USER/remote_apply.sh"

echo "Deploy completado por SSH."