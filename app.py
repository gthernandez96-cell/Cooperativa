import os
import uuid
import logging
from datetime import datetime, timezone, timedelta
UTC = timezone.utc
from flask import Flask, request, session, g, redirect, url_for
from flask_wtf.csrf import CSRFProtect
from flask_limiter import Limiter
from flask_limiter.util import get_remote_address
from dotenv import load_dotenv

# Rate limiter global (se adjunta al app en create_app)
limiter = Limiter(
    key_func=get_remote_address,
    default_limits=[],
    storage_uri="memory://",
)

# Dummy variable for test compatibility
DB = None

# Importar utilidades y configuración
from config import DEFAULT_COOPERATIVA_NOMBRE
from utils.db import get_db, close_db, obtener_marca_cooperativa
from utils.helpers import (
    tipo_transaccion_label, 
    es_transaccion_positiva, 
    limpiar_descripcion_filter
)

# Cargar variables de entorno
load_dotenv()

def create_app():
    app = Flask(__name__)
    
    # Configuración de seguridad
    _secret = os.environ.get('SECRET_KEY')
    if not _secret:
        raise RuntimeError('SECRET_KEY no está definida en el archivo .env')
    app.secret_key = _secret
    
    # Recargar templates automáticamente al detectar cambios (útil en desarrollo)
    app.config['TEMPLATES_AUTO_RELOAD'] = True
    
    # Protección CSRF global
    CSRFProtect(app)

    # Rate limiting
    limiter.init_app(app)

    # Timeout de sesión: 4 horas de inactividad
    SESSION_TIMEOUT_HOURS = 4
    app.config['PERMANENT_SESSION_LIFETIME'] = timedelta(hours=SESSION_TIMEOUT_HOURS)

    # Configurar ProxyFix para manejar HTTPS detrás de proxies (ej. PythonAnywhere)
    from werkzeug.middleware.proxy_fix import ProxyFix
    app.wsgi_app = ProxyFix(app.wsgi_app, x_proto=1, x_host=1)

    # Forzar redirección de HTTP a HTTPS si está configurado o si es producción
    @app.before_request
    def force_https_redirect():
        if app.testing:
            return
        
        force_https = os.environ.get('FORCE_HTTPS', 'False').lower() in ('true', '1', 'yes')
        if not request.is_secure:
            if app.debug and not force_https:
                return
            url = request.url.replace("http://", "https://", 1)
            return redirect(url, code=301)

    # Registro de Blueprints
    from blueprints.auth import bp as auth_bp
    from blueprints.main import bp as main_bp
    from blueprints.usuarios import bp as usuarios_bp
    from blueprints.socios import bp as socios_bp
    from blueprints.ahorro import bp as ahorro_bp
    from blueprints.prestamos import bp as prestamos_bp
    from blueprints.configuraciones import bp as configuraciones_bp
    from blueprints.promotora import bp as promotora_bp
    from blueprints.movimientos import bp as movimientos_bp
    from blueprints.reportes import bp as reportes_bp

    app.register_blueprint(auth_bp)
    app.register_blueprint(main_bp)
    app.register_blueprint(usuarios_bp)
    app.register_blueprint(socios_bp)
    app.register_blueprint(ahorro_bp)
    app.register_blueprint(prestamos_bp)
    app.register_blueprint(configuraciones_bp)
    app.register_blueprint(promotora_bp)
    app.register_blueprint(movimientos_bp)
    app.register_blueprint(reportes_bp)

    # Filtros de Jinja2
    from utils.helpers import formatear_fecha_dmy
    app.jinja_env.filters['tipo_transaccion'] = tipo_transaccion_label
    app.jinja_env.filters['es_transaccion_positiva'] = es_transaccion_positiva
    app.jinja_env.filters['limpiar_descripcion'] = limpiar_descripcion_filter
    app.jinja_env.filters['fecha_dmy'] = formatear_fecha_dmy


    # Procesador de contexto global
    @app.context_processor
    def inject_global_data():
        marca = obtener_marca_cooperativa()
        return {
            'now': datetime.now(),
            'cooperativa_nombre': marca.get('cooperativa_nombre', DEFAULT_COOPERATIVA_NOMBRE),
            'cooperativa_foto': marca.get('cooperativa_foto'),
            **marca
        }

    # Hooks de ciclo de vida de la petición
    @app.before_request
    def _set_request_context():
        g.request_id = request.headers.get('X-Request-ID') or str(uuid.uuid4())
        g.request_started_at = datetime.now(UTC)

        # Verificar timeout de sesión por inactividad
        if 'user_id' in session and not app.testing:
            last_active_str = session.get('_last_active')
            if last_active_str:
                try:
                    last_active = datetime.fromisoformat(last_active_str)
                    if (datetime.now() - last_active).total_seconds() > 4 * 3600:
                        session.clear()
                        return redirect(url_for('auth.login'))
                except (ValueError, TypeError):
                    pass
            session['_last_active'] = datetime.now().isoformat()
            session.modified = True

    @app.after_request
    def _log_request(response):
        started = getattr(g, 'request_started_at', None)
        duration_ms = None
        if started:
            duration_ms = int((datetime.now(UTC) - started).total_seconds() * 1000)
        
        logging.getLogger('cooperativa').info(
            'event=request method=%s path=%s status=%s user=%s role=%s request_id=%s duration_ms=%s',
            request.method, request.path, response.status_code,
            session.get('username', 'anon'), session.get('user_role', 'anon'),
            getattr(g, 'request_id', '-'), duration_ms
        )
        response.headers['X-Request-ID'] = getattr(g, 'request_id', '-')
        return response

    # Cerrar conexión de base de datos automáticamente
    app.teardown_appcontext(close_db)

    return app

# Logging estructurado
logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')

# Instancia global para servidores WSGI
app = create_app()

# Inicializar la base de datos (crea tablas y aplica migraciones)
# Se ejecuta tanto en WSGI como al correr directamente.
with app.app_context():
    from utils.db import init_db
    init_db()

if __name__ == '__main__':
    # Configuración HTTPS para desarrollo local
    use_https = os.environ.get('USE_HTTPS', 'False').lower() in ('true', '1', 'yes')
    ssl_cert = os.environ.get('SSL_CERT_PATH', '').strip()
    ssl_key = os.environ.get('SSL_KEY_PATH', '').strip()
    
    ssl_context = None
    if use_https:
        if ssl_cert and ssl_key:
            ssl_context = (ssl_cert, ssl_key)
        else:
            ssl_context = 'adhoc'
            
    app.run(host='0.0.0.0', port=8001, debug=True, ssl_context=ssl_context)
