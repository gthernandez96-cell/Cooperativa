import urllib.request
import json

data = {
    "planilla_id": 1,
    "pagos": [
        {"prestamo_id": 1, "detalle_id": 1, "numero": "1", "monto": 10}
    ],
    "boleta_deposito": "1231231",
    "nombre_planilla": "Test",
    "fecha_pago": "2026-06-21",
    "frecuencia": "Quincenal"
}

req = urllib.request.Request(
    'http://localhost:8001/pos/procesar_planilla_pos',
    data=json.dumps(data).encode('utf-8'),
    headers={'Content-Type': 'application/json'}
)

try:
    response = urllib.request.urlopen(req)
    print("Success:", response.read().decode('utf-8'))
except urllib.error.HTTPError as e:
    print(f"HTTPError: {e.code}")
    print(e.read().decode('utf-8'))
except Exception as e:
    print(f"Error: {e}")
