# CoopAhorro — Sistema para Cooperativa de Ahorro y Crédito

Sistema web completo construido con Python + Flask + SQLite.

## Requisitos
- macOS con Python 3.9 o superior
- pip (incluido con Python)

## Instalación y ejecución

### 1. Instalar Flask (solo la primera vez)
```bash
pip3 install flask
```

### 2. Entrar a la carpeta del proyecto
```bash
cd cooperativa
```

### 3. Ejecutar la aplicación
```bash
python3 app.py
```

### 4. Abrir en el navegador
```
http://localhost:5000
```

## Funcionalidades

### 👤 Socios
- Registro de nuevos socios con datos completos
- Búsqueda por nombre, código o DPI
- Vista de detalle con cuentas y préstamos asociados

### 🏦 Cuentas
- Apertura de cuentas de ahorro (3.5% anual) y corriente
- Depósitos y retiros con historial completo
- Saldo en tiempo real

### 💰 Préstamos
- Solicitud de crédito con calculadora de cuotas en tiempo real
- Flujo de aprobación (pendiente → aprobado)
- Registro de pagos de cuotas con desglose capital/interés
- Cálculo automático de cuota con fórmula de amortización

## Estructura del proyecto
```
cooperativa/
├── app.py              ← Aplicación Flask principal
├── cooperativa.db      ← Base de datos SQLite (se crea automáticamente)
├── templates/
│   ├── base.html
│   ├── index.html      ← Dashboard
│   ├── socios.html
│   ├── nuevo_socio.html
│   ├── detalle_socio.html
│   ├── cuentas.html
│   ├── nueva_cuenta.html
│   ├── detalle_cuenta.html
│   ├── prestamos.html
│   └── nuevo_prestamo.html
└── README.md
```

## Datos de demostración
Al iniciar, el sistema crea automáticamente 4 socios de ejemplo,
5 cuentas y 3 préstamos para que puedas explorar todas las funciones.

## Guia visual densa (estandar operativo)

Este proyecto usa una escala visual compacta para pantallas de operacion.
La meta es mostrar mas informacion sin perder legibilidad.

### Reglas base (desktop)
- Header global compacto y navegacion con links cortos.
- Contenedores con padding reducido (12px a 14px aprox. por bloque).
- Formularios densos:
	- labels pequenas (11px)
	- inputs/select compactos (padding aprox. 7px 9px)
	- separacion entre campos baja (8px a 10px)
- Tablas densas:
	- encabezados pequenos (10px a 11px)
	- celdas compactas (padding aprox. 7px 10px o 8px 10px)
	- minima decoracion visual
- Botones operativos cortos, priorizando rapidez de uso.

### Reglas base (movil)
- Priorizar densidad sin romper usabilidad tactil.
- Menu lateral con bloques compactos y tipografia controlada.
- .btn y .btn-sm con alturas menores para ganar espacio vertical.
- Margenes/paddings del contenido principal reducidos.

### Referencia actual de implementacion
- Escala global: templates/base.html
- Gestiones (vista operativa consolidada): templates/gestiones.html
- Aprobacion crediticia: templates/aprobar_prestamo.html
- Nuevo retiro: templates/nuevo_retiro.html
- Nuevo prestamo: templates/nuevo_prestamo.html

### Convencion para nuevas vistas
Cuando se cree una pantalla nueva de gestion:
1. Reutilizar clases existentes si aplica (botones, badges, tablas).
2. Mantener la escala densa antes de agregar elementos decorativos.
3. Evitar heroes grandes o bloques con altura excesiva.
4. Validar siempre desktop y movil antes de cerrar cambios.

## Checklist rapido UI (30 segundos)

Use esta lista antes de cerrar cualquier cambio visual.

### 1. Header y navegacion
- [ ] El header no crece de forma innecesaria.
- [ ] Botones de accion usan tamano compacto (btn-sm cuando aplique).
- [ ] No hay textos largos que rompan el layout en desktop.

### 2. Formulario
- [ ] Labels pequenas y legibles (escala densa).
- [ ] Inputs/select con altura compacta y espaciado corto.
- [ ] Botones principales y secundarios alineados y sin exceso de padding.

### 3. Tabla
- [ ] Encabezados compactos (10px a 11px aprox.).
- [ ] Celdas con padding reducido y legibilidad correcta.
- [ ] Acciones por fila visibles sin aumentar alto de fila innecesariamente.

### 4. Movil
- [ ] El menu lateral mantiene escala compacta y usable.
- [ ] Botones tactiles no ocupan mas alto del necesario.
- [ ] El contenido principal entra sin generar bloques vacios grandes.

### 5. Verificacion final
- [ ] Revisado en al menos una vista desktop y una movil.
- [ ] Sin errores de template.
- [ ] Consistencia visual con:
	- templates/base.html
	- templates/gestiones.html
	- templates/aprobar_prestamo.html
	- templates/nuevo_retiro.html
	- templates/nuevo_prestamo.html
