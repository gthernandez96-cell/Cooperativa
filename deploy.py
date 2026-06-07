#!/usr/bin/env python3
"""
deploy.py — Despliegue Total para CoopAhorro en PythonAnywhere.
Empaqueta el código, base de datos e imágenes, lo sube y recarga el servidor.

Uso:
    python deploy.py
"""

import sys
import time
import subprocess
import urllib.request
import urllib.parse
import json
import os
import ssl

# ── Configuración ─────────────────────────────────────────────────────────────
PA_USER      = "gthernandez96"
PA_API_TOKEN = "1e628583fd7c3b8aeadcf91b5ae29aa820f6daea"
PA_DOMAIN    = f"{PA_USER}.pythonanywhere.com"
BASE_URL     = f"https://www.pythonanywhere.com/api/v0/user/{PA_USER}"
HEADERS      = {"Authorization": f"Token {PA_API_TOKEN}"}
ZIP_NAME     = "coop_deploy.zip"
ZIP_PATH     = f"/tmp/{ZIP_NAME}"
# ─────────────────────────────────────────────────────────────────────────────

def pa_request(method, endpoint, data=None, expect_json=True):
    url = f"{BASE_URL}{endpoint}"
    body = urllib.parse.urlencode(data).encode() if data else None
    req = urllib.request.Request(url, data=body, headers=HEADERS, method=method)
    context = ssl._create_unverified_context()
    try:
        with urllib.request.urlopen(req, timeout=45, context=context) as r:
            raw = r.read()
            if not raw or not expect_json: return True
            return json.loads(raw)
    except Exception as e:
        print(f"  ✗ Error en API PythonAnywhere: {e}")
        if hasattr(e, 'read'):
            try:
                print(f"    Detalles del error: {e.read().decode()}")
            except:
                pass
        return None

def pack_project():
    print(f"\n📦 [1/4] Empaquetando el proyecto local...")
    if os.path.exists(ZIP_PATH):
        os.remove(ZIP_PATH)
    
    # Comprimir ignorando carpetas pesadas/innecesarias
    cmd = [
        "zip", "-q", "-r", ZIP_PATH, ".", 
        "-x", "*.git*", "*.venv*", "*__pycache__*", "*.pytest_cache*", "*instance*"
    ]
    r = subprocess.run(cmd)
    if r.returncode == 0:
        size = os.path.getsize(ZIP_PATH) / (1024 * 1024)
        print(f"  ✓ Empaquetado listo ({size:.1f} MB)")
        return True
    print("  ✗ Error al empaquetar")
    return False

def upload_zip():
    print(f"\n☁️ [2/4] Subiendo a PythonAnywhere...")
    with open(ZIP_PATH, "rb") as fh:
        data = fh.read()

    boundary = b"----boundary"
    body  = b"--" + boundary + b"\r\n"
    body += b'Content-Disposition: form-data; name="content"; filename="file"\r\n'
    body += b"Content-Type: application/zip\r\n\r\n"
    body += data + b"\r\n"
    body += b"--" + boundary + b"--\r\n"

    req = urllib.request.Request(
        f"{BASE_URL}/files/path/home/{PA_USER}/{ZIP_NAME}",
        data=body,
        headers={
            "Authorization": f"Token {PA_API_TOKEN}",
            "Content-Type": "multipart/form-data; boundary=----boundary",
        },
        method="POST"
    )
    context = ssl._create_unverified_context()

    try:
        with urllib.request.urlopen(req, context=context) as r:
            print("  ✓ Archivo subido exitosamente")
            return True
    except Exception as e:
        print(f"  ✗ Error al subir: {e}")
        return False

def extract_and_install():
    print("\n⚙️ [3/4] Descomprimiendo y actualizando el servidor...")
    
    console = pa_request("POST", "/consoles/", {"executable": "bash"})
    if not console:
        consoles = pa_request("GET", "/consoles/")
        if consoles: console = consoles[0]
        else: return False

    cid = console["id"]
    time.sleep(2)
    
    script = f"""
    cd /home/{PA_USER}
    mkdir -p Cooperativa
    cd Cooperativa
    unzip -q -o ../{ZIP_NAME}
    if [ ! -d ".venv" ]; then python3.12 -m venv .venv; fi
    .venv/bin/pip install -q -r requirements.txt
    rm ../{ZIP_NAME}
    echo "=== DONE ==="
    """
    
    pa_request("POST", f"/consoles/{cid}/send_input/", {"input": script + "\n"})
    
    print("  ⏳ Procesando (esto toma unos 15 segundos)", end="")
    success = False
    for _ in range(8):
        time.sleep(4)
        print(".", end="", flush=True)
        out = pa_request("GET", f"/consoles/{cid}/get_latest_output/")
        if out and "output" in out and "=== DONE ===" in out["output"]:
            success = True
            break
            
    print("\n  ✓ Servidor actualizado" if success else "\n  ⚠ El proceso sigue en segundo plano")
    pa_request("DELETE", f"/consoles/{cid}/")
    return True

def reload_webapp():
    print("\n🔄 [4/4] Recargando aplicación web...")
    if pa_request("POST", f"/webapps/{PA_DOMAIN}/reload/", expect_json=False):
        print("  ✓ Aplicación recargada")
        return True
    return False

def main():
    print("╔══════════════════════════════════════════════════════╗")
    print("║    CoopAhorro — Deploy Automático a Producción       ║")
    print("╚══════════════════════════════════════════════════════╝")
    
    if pack_project() and upload_zip() and extract_and_install() and reload_webapp():
        print(f"\n✅ ¡Deploy completado con éxito!")
        print(f"   → https://{PA_DOMAIN}")
    else:
        print(f"\n❌ El deploy falló. Revisa los errores arriba.")

if __name__ == "__main__":
    main()
