import sqlite3

def run_migration():
    conn = sqlite3.connect('cooperativa.db')
    c = conn.cursor()

    try:
        c.execute("ALTER TABLE usuarios ADD COLUMN bodega_id INTEGER REFERENCES pos_bodegas(id)")
    except sqlite3.OperationalError:
        pass # Column already exists
    
    # Assign default bodega to all users who don't have one
    c.execute("SELECT id FROM pos_bodegas ORDER BY id LIMIT 1")
    row = c.fetchone()
    if row:
        bodega_id = row[0]
        c.execute("UPDATE usuarios SET bodega_id=? WHERE bodega_id IS NULL", (bodega_id,))

    conn.commit()
    conn.close()
    print("Usuarios migracion completada.")

if __name__ == '__main__':
    run_migration()
