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
ZIP_PATH     = ZIP_NAME
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
    
    # Comprimir ignorando carpetas pesadas/innecesarias y el propio zip
    cmd = [
        "zip", "-q", "-r", ZIP_PATH, ".", 
        "-x", "*.git*", "*.venv*", "*__pycache__*", "*.pytest_cache*", "*instance*", f"*{ZIP_NAME}*"
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
    print("\n⚙️ [3/4] Configurando auto-extractor en archivo WSGI...")
    
    wsgi_path = f"/var/www/{PA_USER}_pythonanywhere_com_wsgi.py"
    url = f"{BASE_URL}/files/path{wsgi_path}"
    
    # 1. Leer archivo WSGI existente
    req = urllib.request.Request(url, headers=HEADERS, method="GET")
    context = ssl._create_unverified_context()
    try:
        with urllib.request.urlopen(req, context=context) as r:
            current_content = r.read().decode('utf-8')
    except Exception as e:
        print(f"  ✗ Error al leer WSGI: {e}")
        return False
        
    # 2. Validar si el auto-extractor ya está configurado
    AUTO_EXTRACT_MARKER = "# === DEPLOYMENT AUTO-EXTRACTOR ==="
    if AUTO_EXTRACT_MARKER in current_content:
        print("  ✓ Auto-extractor ya está configurado en el archivo WSGI")
        return True
        
    # 3. Insertar el auto-extractor en la parte superior del archivo WSGI
    auto_extract_code = f"""{AUTO_EXTRACT_MARKER}
import os
import zipfile
import subprocess

zip_path = '/home/{PA_USER}/{ZIP_NAME}'
dest_dir = '/home/{PA_USER}/Cooperativa'

if os.path.exists(zip_path):
    try:
        print("Detectado archivo de despliegue. Descomprimiendo...")
        with zipfile.ZipFile(zip_path, 'r') as zip_ref:
            zip_ref.extractall(dest_dir)
            
        venv_pip = os.path.join(dest_dir, '.venv', 'bin', 'pip')
        req_txt = os.path.join(dest_dir, 'requirements.txt')
        if os.path.exists(venv_pip) and os.path.exists(req_txt):
            subprocess.run([venv_pip, 'install', '-r', req_txt], capture_output=True)
            
        os.remove(zip_path)
        print("Despliegue completado exitosamente.")
    except Exception as e:
        print("Error en auto-extractor:", e)
# =================================

"""
    new_content = auto_extract_code + current_content
    
    # 4. Subir archivo WSGI modificado mediante POST
    boundary = b"----boundary"
    body  = b"--" + boundary + b"\r\n"
    body += b'Content-Disposition: form-data; name="content"; filename="wsgi.py"\r\n'
    body += b"Content-Type: text/plain\r\n\r\n"
    body += new_content.encode('utf-8') + b"\r\n"
    body += b"--" + boundary + b"--\r\n"
    
    req_upload = urllib.request.Request(
        url,
        data=body,
        headers={
            "Authorization": f"Token {PA_API_TOKEN}",
            "Content-Type": "multipart/form-data; boundary=----boundary",
        },
        method="POST"
    )
    
    try:
        with urllib.request.urlopen(req_upload, context=context) as r:
            print("  ✓ Auto-extractor inyectado exitosamente en archivo WSGI")
            return True
    except Exception as e:
        print(f"  ✗ Error al guardar WSGI: {e}")
        return False


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
    
    try:
        if pack_project() and upload_zip() and extract_and_install() and reload_webapp():
            print(f"\n✅ ¡Deploy completado con éxito!")
            print(f"   → https://{PA_DOMAIN}")
        else:
            print(f"\n❌ El deploy falló. Revisa los errores arriba.")
    finally:
        if os.path.exists(ZIP_PATH):
            print(f"\n🧹 Limpiando archivo temporal local {ZIP_PATH}...")
            os.remove(ZIP_PATH)

if __name__ == "__main__":
    main()
