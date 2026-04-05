"""
utils/images.py — Procesamiento de imágenes (socios y cooperativa).
"""
import os
from datetime import datetime
from werkzeug.utils import secure_filename
from config import SOCIOS_UPLOAD_DIR, COOPERATIVA_UPLOAD_DIR, ALLOWED_IMAGE_EXTENSIONS


def allowed_image(filename):
    """Valida que la extensión del archivo sea una imagen permitida."""
    if not filename or '.' not in filename:
        return False
    return filename.rsplit('.', 1)[1].lower() in ALLOWED_IMAGE_EXTENSIONS


# Alias para compatibilidad con código existente
allowed_socio_image = allowed_image
allowed_system_image = allowed_image


def procesar_foto_socio(foto, sid):
    """Redimensiona y guarda la foto de un socio. Devuelve la ruta relativa."""
    ext = secure_filename(foto.filename).rsplit('.', 1)[1].lower()
    nombre_archivo = f"socio_{sid}_{int(datetime.now().timestamp())}.{ext}"
    os.makedirs(SOCIOS_UPLOAD_DIR, exist_ok=True)
    ruta_archivo = os.path.join(SOCIOS_UPLOAD_DIR, nombre_archivo)

    try:
        from PIL import Image, ImageOps
        with Image.open(foto.stream) as img:
            img = ImageOps.exif_transpose(img)
            if img.mode not in ('RGB', 'RGBA'):
                img = img.convert('RGB')
            img_fit = ImageOps.fit(img, (512, 512), method=Image.Resampling.LANCZOS, centering=(0.5, 0.5))
            if ext in ('jpg', 'jpeg'):
                if img_fit.mode != 'RGB':
                    img_fit = img_fit.convert('RGB')
                img_fit.save(ruta_archivo, format='JPEG', quality=88, optimize=True)
            elif ext == 'webp':
                if img_fit.mode != 'RGB':
                    img_fit = img_fit.convert('RGB')
                img_fit.save(ruta_archivo, format='WEBP', quality=88, optimize=True)
            else:
                img_fit.save(ruta_archivo, format='PNG', optimize=True)
    except ModuleNotFoundError:
        foto.stream.seek(0)
        foto.save(ruta_archivo)
    except Exception as e:
        raise ValueError(f'No se pudo procesar la imagen: {e}')

    return f"uploads/socios/{nombre_archivo}"


def procesar_foto_cooperativa(foto):
    """Redimensiona y guarda el logo de la cooperativa. Devuelve la ruta relativa."""
    ext = secure_filename(foto.filename).rsplit('.', 1)[1].lower()
    nombre_archivo = f"cooperativa_{int(datetime.now().timestamp())}.{ext}"
    os.makedirs(COOPERATIVA_UPLOAD_DIR, exist_ok=True)
    ruta_archivo = os.path.join(COOPERATIVA_UPLOAD_DIR, nombre_archivo)

    try:
        from PIL import Image, ImageOps
        with Image.open(foto.stream) as img:
            img = ImageOps.exif_transpose(img)
            if img.mode not in ('RGB', 'RGBA'):
                img = img.convert('RGBA' if ext == 'png' else 'RGB')
            img.thumbnail((640, 640), Image.Resampling.LANCZOS)
            if ext == 'webp':
                if img.mode != 'RGB':
                    img = img.convert('RGB')
                img.save(ruta_archivo, format='WEBP', quality=90, optimize=True)
            elif ext in ('jpg', 'jpeg'):
                if img.mode != 'RGB':
                    img = img.convert('RGB')
                img.save(ruta_archivo, format='JPEG', quality=90, optimize=True)
            else:
                img.save(ruta_archivo, format='PNG', optimize=True)
    except ModuleNotFoundError:
        foto.stream.seek(0)
        foto.save(ruta_archivo)
    except Exception as e:
        raise ValueError(f'No se pudo procesar la imagen institucional: {e}')

    return f"uploads/cooperativa/{nombre_archivo}"
