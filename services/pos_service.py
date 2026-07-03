import os
from datetime import datetime
from utils.db import get_db, db_fetchone, db_fetchall, db_execute, db_insert_and_get_id

def realizar_traslado(origen_id, destino_id, notas, items, usuario):
    """
    Realiza un traslado atómico de inventario entre bodegas.
    Retorna (success, data) donde data es un dict con 'numero' si success es True, o 'error' si es False.
    """
    if not origen_id or not destino_id or origen_id == destino_id or not items:
        return False, {'error': 'Datos de traslado inválidos.'}
        
    conn = get_db()
    try:
        # Bloqueo atómico lógico para la transacción en SQLite
        n_tras_row = db_fetchone(conn, "SELECT COUNT(*) FROM pos_traslados")
        n_tras = n_tras_row[0] + 1 if n_tras_row else 1
        num_tras = f"TRS-{n_tras:06d}"
        fecha = datetime.now().isoformat()
        
        # Validar stock en origen de forma estricta
        for item in items:
            pid = int(item['producto_id'])
            cant = float(item['cantidad'])
            row_stock = db_fetchone(conn, "SELECT stock FROM pos_producto_bodegas WHERE producto_id=? AND bodega_id=?", (pid, origen_id))
            stock_actual = float(row_stock['stock']) if row_stock else 0
            if cant > stock_actual:
                prod = db_fetchone(conn, "SELECT nombre FROM pos_productos WHERE id=?", (pid,))
                nombre_prod = prod['nombre'] if prod else f'Prod {pid}'
                return False, {'error': f"Stock insuficiente en origen para {nombre_prod} (Disp: {stock_actual})"}
                
        # Crear registro maestro del traslado
        tras_id = db_insert_and_get_id(conn, """
            INSERT INTO pos_traslados (numero, origen_bodega_id, destino_bodega_id, fecha, usuario, estado, notas)
            VALUES (?,?,?,?,?,?,?)
        """, (num_tras, origen_id, destino_id, fecha, usuario, 'completado', notas))
        
        # Procesar detalles y mover el stock de manera atómica
        for item in items:
            pid = int(item['producto_id'])
            cant = float(item['cantidad'])
            db_execute(conn, "INSERT INTO pos_traslado_detalles (traslado_id, producto_id, cantidad) VALUES (?,?,?)", (tras_id, pid, cant))
            
            # Descontar origen
            db_execute(conn, "UPDATE pos_producto_bodegas SET stock = stock - ? WHERE producto_id=? AND bodega_id=?", (cant, pid, origen_id))
            
            # Aumentar destino (crear si no existe en la bodega destino)
            row_dest = db_fetchone(conn, "SELECT stock FROM pos_producto_bodegas WHERE producto_id=? AND bodega_id=?", (pid, destino_id))
            if row_dest:
                db_execute(conn, "UPDATE pos_producto_bodegas SET stock = stock + ? WHERE producto_id=? AND bodega_id=?", (cant, pid, destino_id))
            else:
                db_execute(conn, "INSERT INTO pos_producto_bodegas (producto_id, bodega_id, stock) VALUES (?,?,?)", (pid, destino_id, cant))
            
        conn.commit()
        return True, {'numero': num_tras}
    except Exception as e:
        conn.rollback()
        return False, {'error': str(e)}
    finally:
        conn.close()
