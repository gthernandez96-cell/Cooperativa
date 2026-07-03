import sqlite3
from datetime import datetime

def run_migration():
    conn = sqlite3.connect('cooperativa.db')
    c = conn.cursor()

    # 1. Create tables
    c.execute("""
    CREATE TABLE IF NOT EXISTS pos_bodegas (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        nombre TEXT UNIQUE NOT NULL,
        ubicacion TEXT,
        estado TEXT DEFAULT 'activo'
    )
    """)

    c.execute("""
    CREATE TABLE IF NOT EXISTS pos_producto_bodegas (
        producto_id INTEGER NOT NULL,
        bodega_id INTEGER NOT NULL,
        stock REAL DEFAULT 0,
        PRIMARY KEY (producto_id, bodega_id),
        FOREIGN KEY (producto_id) REFERENCES pos_productos(id),
        FOREIGN KEY (bodega_id) REFERENCES pos_bodegas(id)
    )
    """)

    c.execute("""
    CREATE TABLE IF NOT EXISTS pos_traslados (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        numero TEXT UNIQUE NOT NULL,
        origen_bodega_id INTEGER NOT NULL,
        destino_bodega_id INTEGER NOT NULL,
        fecha TEXT NOT NULL,
        usuario TEXT,
        estado TEXT DEFAULT 'completado',
        notas TEXT,
        FOREIGN KEY (origen_bodega_id) REFERENCES pos_bodegas(id),
        FOREIGN KEY (destino_bodega_id) REFERENCES pos_bodegas(id)
    )
    """)

    c.execute("""
    CREATE TABLE IF NOT EXISTS pos_traslado_detalles (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        traslado_id INTEGER NOT NULL,
        producto_id INTEGER NOT NULL,
        cantidad REAL NOT NULL,
        FOREIGN KEY (traslado_id) REFERENCES pos_traslados(id),
        FOREIGN KEY (producto_id) REFERENCES pos_productos(id)
    )
    """)

    # 2. Modify pos_compras (add numero_factura, bodega_id)
    try:
        c.execute("ALTER TABLE pos_compras ADD COLUMN numero_factura TEXT")
    except sqlite3.OperationalError:
        pass # Column exists

    try:
        c.execute("ALTER TABLE pos_compras ADD COLUMN bodega_id INTEGER REFERENCES pos_bodegas(id)")
    except sqlite3.OperationalError:
        pass

    # 3. Modify pos_caja_sesiones
    try:
        c.execute("ALTER TABLE pos_caja_sesiones ADD COLUMN bodega_id INTEGER REFERENCES pos_bodegas(id)")
    except sqlite3.OperationalError:
        pass

    # 4. Create default warehouse and migrate data
    c.execute("SELECT id FROM pos_bodegas WHERE nombre='Bodega Principal'")
    row = c.fetchone()
    if not row:
        c.execute("INSERT INTO pos_bodegas (nombre, ubicacion) VALUES ('Bodega Principal', 'Sede Central')")
        bodega_id = c.lastrowid
    else:
        bodega_id = row[0]

    # Assign all current pos_caja_sesiones to the main warehouse to avoid breaks
    c.execute("UPDATE pos_caja_sesiones SET bodega_id=? WHERE bodega_id IS NULL", (bodega_id,))

    # Assign all existing compras to main warehouse
    c.execute("UPDATE pos_compras SET bodega_id=? WHERE bodega_id IS NULL", (bodega_id,))

    # Assign all current stock to this warehouse
    c.execute("SELECT id, stock FROM pos_productos")
    productos = c.fetchall()
    for pid, stock in productos:
        # insert or replace
        c.execute("INSERT OR IGNORE INTO pos_producto_bodegas (producto_id, bodega_id, stock) VALUES (?, ?, ?)", (pid, bodega_id, stock))

    conn.commit()
    conn.close()
    print("Migración completada con éxito.")

if __name__ == '__main__':
    run_migration()
