#!/usr/bin/env python3
"""
deploy.py — Despliega CoopAhorro a PythonAnywhere (cuenta gratuita).

Uso:
    python deploy.py              # git push + abre consola PA + recarga webapp
    python deploy.py --no-push    # Solo abre consola PA + recarga webapp
    python deploy.py --only-push  # Solo git push
    python deploy.py --reload     # Solo recarga la webapp (ya hiciste git pull)
"""

import sys
import time
import subprocess
import argparse
import urllib.request
import urllib.parse
import json

# ── Configuración ─────────────────────────────────────────────────────────────
PA_USER      = "gthernandez96"
PA_API_TOKEN = "1e628583fd7c3b8aeadcf91b5ae29aa820f6daea"
PA_DOMAIN    = f"{PA_USER}.pythonanywhere.com"
PA_APP_DIR   = f"/home/{PA_USER}/Cooperativa"
PA_VENV      = f"{PA_APP_DIR}/.venv"
BASE_URL     = f"https://www.pythonanywhere.com/api/v0/user/{PA_USER}"
HEADERS      = {"Authorization": f"Token {PA_API_TOKEN}"}
PA_CONSOLE_URL = "https://www.pythonanywhere.com/user/gthernandez96/consoles/"
# ─────────────────────────────────────────────────────────────────────────────


def pa_request(method, endpoint, data=None, expect_json=True):
    url = f"{BASE_URL}{endpoint}"
    body = urllib.parse.urlencode(data).encode() if data else None
    req = urllib.request.Request(url, data=body, headers=HEADERS, method=method)
    try:
        with urllib.request.urlopen(req, timeout=30) as r:
            raw = r.read()
            if not raw or not expect_json:
                return True
            return json.loads(raw)
    except urllib.error.HTTPError as e:
        err = e.read().decode()
        print(f"  ✗ Error HTTP {e.code}: {err}")
        return None


def git_push():
    print("\n📦 [1/3] Subiendo cambios a GitHub...")
    r = subprocess.run(["git", "push", "origin", "main"],
                       capture_output=True, text=True)
    if r.returncode == 0:
        print("  ✓ git push completado")
        return True
    if "up-to-date" in r.stderr or "up to date" in r.stderr:
        print("  ✓ Ya estaba actualizado en GitHub")
        return True
    print(f"  ✗ git push falló:\n{r.stderr}")
    return False


def open_pa_console():
    """Abre la consola de PythonAnywhere en el navegador."""
    print(f"\n🌐 [2/3] Abriendo la consola de PythonAnywhere en el navegador...")
    subprocess.run(["open", PA_CONSOLE_URL])
    print(f"  ✓ Consola abierta: {PA_CONSOLE_URL}")
    print()
    print("  ┌─────────────────────────────────────────────────────────┐")
    print("  │  En la consola Bash de PythonAnywhere, ejecuta:         │")
    print(f"  │  cd ~/Cooperativa && git pull origin main               │")
    print("  │                                                         │")
    print("  │  Luego regresa aquí y presiona ENTER para continuar.    │")
    print("  └─────────────────────────────────────────────────────────┘")
    input("\n  ▶ Presiona ENTER cuando hayas ejecutado git pull... ")


def reload_webapp():
    print("\n🔄 [3/3] Recargando aplicación web en PythonAnywhere...")
    result = pa_request("POST", f"/webapps/{PA_DOMAIN}/reload/", expect_json=False)
    if result:
        print("  ✓ Aplicación recargada exitosamente")
        return True
    print("  ✗ No se pudo recargar — recárgala manualmente desde la pestaña Web")
    return False


def main():
    parser = argparse.ArgumentParser(description="Deploy CoopAhorro → PythonAnywhere (free)")
    parser.add_argument("--no-push",   action="store_true", help="No hacer git push")
    parser.add_argument("--only-push", action="store_true", help="Solo git push")
    parser.add_argument("--reload",    action="store_true", help="Solo recargar webapp")
    args = parser.parse_args()

    print()
    print("╔══════════════════════════════════════════════════════╗")
    print("║    CoopAhorro — Deploy a PythonAnywhere (free)      ║")
    print(f"║    → https://{PA_DOMAIN}   ║")
    print("╚══════════════════════════════════════════════════════╝")

    # Modo: solo reload
    if args.reload:
        reload_webapp()
        print(f"\n✅ Listo. Visita: https://{PA_DOMAIN}")
        return

    # Modo: solo push
    if args.only_push:
        git_push()
        return

    # Flujo completo
    if not args.no_push:
        ok = git_push()
        if not ok:
            print("\n⚠ git push falló. Abortando.")
            sys.exit(1)

    open_pa_console()
    reload_webapp()

    print(f"\n✅ ¡Deploy completado!")
    print(f"   → https://{PA_DOMAIN}")


if __name__ == "__main__":
    main()
