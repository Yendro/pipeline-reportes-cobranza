import sys
import os
import datetime
import logging
from pathlib import Path
from dotenv import load_dotenv

load_dotenv()

# Rutas base
BASE_DIR = Path(__file__).parent.parent if Path(__file__).parent.name == 'src' else Path(__file__).parent
DATA_DIR = BASE_DIR / 'data'
REPORTS_DIR = BASE_DIR / 'reports'

# Configuración BigQuery
BIGQUERY_CREDENTIALS_PATH = Path(os.getenv('BIGQUERY_CREDENTIALS_PATH'))
PROJECT_ID = os.getenv('BIGQUERY_PROJECT_ID')

def configurar_logger():
    """Configurar sistema de logging"""
    logging.basicConfig(
        level=logging.INFO,
        format='%(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler('logs.log'),
            logging.StreamHandler(sys.stdout)
        ]
    )

def obtener_consultas_sql():
    archivos_sql = list(DATA_DIR.glob('*.sql'))
    return archivos_sql

def crear_carpeta_reporte():
    fecha_actual = datetime.date.today()
    
    # Formatear el nombre de la carpeta
    nombre_carpeta = f"PV-{fecha_actual.year}-{fecha_actual.month:02d}-{fecha_actual.day:02d}"
    carpeta_reporte = REPORTS_DIR / nombre_carpeta
    
    # Crear la carpeta (y la carpeta reports si no existe)
    carpeta_reporte.mkdir(parents=True, exist_ok=True)
    return carpeta_reporte

def obtener_archivo_de_salida(base_nombre):
    return f"{base_nombre}.xlsx"

# Función de conveniencia para obtener la carpeta de reporte actual
def obtener_carpeta_reporte_actual():
    """Obtener la carpeta de reporte actual sin crearla"""
    fecha_actual = datetime.date.today()
    nombre_carpeta = f"PV-{fecha_actual.year}-{fecha_actual.month:02d}-{fecha_actual.day:02d}"
    return REPORTS_DIR / nombre_carpeta

# Opcional: Función para limpiar carpeta si se desea
def limpiar_carpeta_reporte(carpeta):
    """Limpiar todos los archivos de una carpeta de reporte (opcional)"""
    if carpeta.exists():
        for archivo in carpeta.glob("*"):
            try:
                archivo.unlink()
            except Exception as e:
                print(f"No se pudo eliminar {archivo}: {e}")
    return carpeta