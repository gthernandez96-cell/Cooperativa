#!/usr/bin/env python3
"""
pull_db.py — Descarga la base de datos de producción desde PythonAnywhere
para sincronizar los datos creados en la web (socios, transacciones, etc.) localmente.
"""
import os
import shutil
import time
import urllib.request
import ssl

PA_USER      = "gthernandez96"
PA_API_TOKEN = "1e628583fd7c3b8aeadcf91b5ae29aa820f6daea"
BASE_URL     = f"https://www.pythonanywhere.com/api/v0/user/{PA_USER}"
HEADERS      = {"Authorization": f"Token {PA_API_TOKEN}"}

LOCAL_DB_PATH = "cooperativa.db"
BACKUP_DB_PATH = f"cooperativa_backup_{int(time.time())}.db"

def backup_local_db():
    if os.path.exists(LOCAL_DB_PATH):
        print(f"📦 Creando respaldo de la base de datos local en {BACKUP_DB_PATH}...")
        shutil.copy2(LOCAL_DB_PATH, BACKUP_DB_PATH)
        print("✓ Respaldo local creado.")
    else:
        print("ℹ No existe base de datos local previa, se creará una nueva.")

def download_remote_db():
    print(f"☁️ Descargando cooperativa.db desde PythonAnywhere...")
    # La ruta de la base de datos en PythonAnywhere
    remote_db_path = f"/home/{PA_USER}/Cooperativa/cooperativa.db"
    url = f"{BASE_URL}/files/path{remote_db_path}"
    
    req = urllib.request.Request(url, headers=HEADERS, method="GET")
    context = ssl._create_unverified_context()
    
    try:
        with urllib.request.urlopen(req, context=context) as r:
            db_data = r.read()
            
        with open(LOCAL_DB_PATH, "wb") as f:
            f.write(db_data)
            
        print("✓ Base de datos de producción descargada y aplicada localmente con éxito.")
        return True
    except Exception as e:
        print(f"✗ Error al descargar la base de datos: {e}")
        return False

def main():
    print("╔══════════════════════════════════════════════════════╗")
    print("║   CoopAhorro — Sincronizar Base de Datos a Local     ║")
    print("╚══════════════════════════════════════════════════════╝")
    
    backup_local_db()
    if download_remote_db():
        print("\nSincronización completada. Ya puedes ver los asociados creados en producción ejecutando localmente.")

if __name__ == "__main__":
    main()
