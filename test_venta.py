import json
from app import create_app
from utils.db import get_db

app = create_app()
app.config['WTF_CSRF_ENABLED'] = False
app.testing = True

with app.test_client() as client:
    # Login
    res = client.post('/login', data={'username': 'admin', 'password': 'admin123'})
    
    # Abrir caja si no esta abierta
    # Solo asumimos que hay caja abierta por la peticion del usuario o hacemos una post
    # Pero si falla guardar venta, la caja deberia estar abierta
    
    data = {
        'cliente_nombre': 'CF',
        'cliente_nit': 'CF',
        'cliente_direccion': 'Ciudad',
        'items': [{'id': 1, 'cantidad': 1, 'precio': 10}],
        'pagos': [{'metodo': 'efectivo', 'monto': 10}],
        'total': 10
    }
    
    res = client.post('/pos/guardar_venta', json=data)
    print("STATUS CODE:", res.status_code)
    try:
        print("JSON RESPONSE:", res.json)
    except:
        print("TEXT RESPONSE:", res.data.decode('utf-8'))
