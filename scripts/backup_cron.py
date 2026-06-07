#!/usr/bin/env python3
"""
backup_cron.py — Backup automático de la base de datos SQLite de CoopAhorro.

Uso:
    python scripts/backup_cron.py

Programar en PythonAnywhere (Scheduled Tasks):
    Hora: cualquier hora de baja actividad (ej. 02:00 AM)
    Comando: /home/<usuario>/.virtualenvs/<env>/bin/python /home/<usuario>/cooperativa/scripts/backup_cron.py

Programar localmente con cron (crontab -e):
    0 2 * * * /ruta/al/.venv/bin/python /ruta/al/proyecto/scripts/backup_cron.py >> /ruta/logs/backup.log 2>&1
"""

import os
import sys
import shutil
import glob
from datetime import date, timedelta

# ── Configuración ──────────────────────────────────────────────────────────────
# Ruta base del proyecto (se asume que el script está en scripts/)
BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))

# Ruta de la BD fuente
DB_PATH = os.path.join(BASE_DIR, 'cooperativa.db')

# Directorio donde se guardan los backups
BACKUP_DIR = os.path.join(BASE_DIR, 'backups')

# Número de días que se conservan los backups antes de eliminarlos
RETENTION_DAYS = 30

# ── Lógica principal ───────────────────────────────────────────────────────────

def main():
    hoy = date.today().isoformat()
    print(f"[{hoy}] Iniciando backup de CoopAhorro...")

    # 1. Verificar que la BD existe
    if not os.path.isfile(DB_PATH):
        print(f"  ERROR: No se encontró la base de datos en: {DB_PATH}")
        sys.exit(1)

    # 2. Crear directorio de backups si no existe
    os.makedirs(BACKUP_DIR, exist_ok=True)

    # 3. Copiar la BD con fecha en el nombre
    backup_filename = f"cooperativa_backup_{hoy}.db"
    backup_path = os.path.join(BACKUP_DIR, backup_filename)

    try:
        shutil.copy2(DB_PATH, backup_path)
        size_kb = os.path.getsize(backup_path) / 1024
        print(f"  ✓ Backup creado: {backup_filename} ({size_kb:.1f} KB)")
    except Exception as e:
        print(f"  ERROR al crear el backup: {e}")
        sys.exit(1)

    # 4. Eliminar backups más antiguos que RETENTION_DAYS días
    limite = date.today() - timedelta(days=RETENTION_DAYS)
    patron = os.path.join(BACKUP_DIR, 'cooperativa_backup_*.db')
    eliminados = 0

    for archivo in glob.glob(patron):
        nombre = os.path.basename(archivo)
        # Extraer fecha del nombre: cooperativa_backup_YYYY-MM-DD.db
        partes = nombre.replace('cooperativa_backup_', '').replace('.db', '')
        try:
            fecha_backup = date.fromisoformat(partes)
            if fecha_backup < limite:
                os.remove(archivo)
                print(f"  🗑  Backup antiguo eliminado: {nombre}")
                eliminados += 1
        except (ValueError, OSError):
            pass  # Ignorar archivos con nombre no estándar

    if eliminados == 0:
        print(f"  ✓ No hay backups antiguos que limpiar (retención: {RETENTION_DAYS} días)")
    else:
        print(f"  ✓ {eliminados} backup(s) antiguo(s) eliminado(s)")

    # 5. Listar backups actuales
    backups_actuales = sorted(glob.glob(patron), reverse=True)
    print(f"  ✓ Total de backups conservados: {len(backups_actuales)}")
    print(f"[{hoy}] Backup completado exitosamente.")


if __name__ == '__main__':
    main()
