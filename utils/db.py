"""
utils/db.py — Gestión de conexiones a base de datos usando Flask's g.

Usar get_db() en cada ruta obtiene la misma conexión dentro de la
request, y close_db() la cierra automáticamente al terminar.
"""
import sqlite3
from flask import g
from config import DB


def get_db():
    """Devuelve la conexión de BD del contexto de la request actual (Flask g).
    Si no existe, la crea. Garantiza una sola conexión por request."""
    if 'db' not in g:
        conn = sqlite3.connect(DB, timeout=30)
        conn.row_factory = sqlite3.Row
        conn.execute('PRAGMA busy_timeout = 30000')
        conn.execute('PRAGMA journal_mode = WAL')
        g.db = conn
    return g.db


def close_db(error=None):
    """Cierra la conexión de BD al terminar la request. Registrar con app.teardown_appcontext."""
    db = g.pop('db', None)
    if db is not None:
        db.close()
