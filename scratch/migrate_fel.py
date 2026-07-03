import sqlite3
import os

db_path = 'cooperativa.db'

if not os.path.exists(db_path):
    print("Database not found.")
    exit(1)

conn = sqlite3.connect(db_path)
c = conn.cursor()

def add_column(table, col_name, col_def):
    try:
        c.execute(f"ALTER TABLE {table} ADD COLUMN {col_name} {col_def}")
        print(f"Added {col_name} to {table}")
    except sqlite3.OperationalError as e:
        if "duplicate column name" in str(e):
            print(f"Column {col_name} already exists in {table}")
        else:
            print(f"Error adding {col_name}: {e}")

add_column('pos_ventas', 'fel_uuid', 'TEXT')
add_column('pos_ventas', 'fel_serie', 'TEXT')
add_column('pos_ventas', 'fel_numero', 'TEXT')
add_column('pos_ventas', 'fel_fecha_certificacion', 'TEXT')
add_column('pos_ventas', 'cliente_nit', 'TEXT DEFAULT "CF"')
add_column('pos_ventas', 'cliente_direccion', 'TEXT DEFAULT "Ciudad"')

conn.commit()
conn.close()
print("Migration completed.")
