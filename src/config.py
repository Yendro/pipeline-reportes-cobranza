import sys
import os
import datetime
import logging
from pathlib import Path
from dotenv import load_dotenv

load_dotenv()

# Rutas base
BASE_DIR = Path(__file__).parent.parent
DATA_DIR = BASE_DIR / 'data'
REPORTS_DIR = BASE_DIR / 'reports'
LOG_FILE = BASE_DIR / 'logs.log'

# Configuración BigQuery
BIGQUERY_CREDENTIALS_PATH = Path(os.getenv('BIGQUERY_CREDENTIALS_PATH'))
PROJECT_ID = os.getenv('BIGQUERY_PROJECT_ID')

def configurar_logger():
    """Configurar sistema de logging"""
    logging.basicConfig(
        level=logging.INFO,
        format='%(levelname)s - %(message)s',
        # format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler(LOG_FILE, encoding='utf-8'),
            logging.StreamHandler(sys.stdout)
        ]
    )

def obtener_consultas_sql():
    """Obtener todos los archivos SQL de la carpeta data"""
    return list(DATA_DIR.glob('*.sql'))

def crear_carpeta_reporte():
    """Crear carpeta con nombre basado en fecha actual"""
    fecha_actual = datetime.date.today()
    nombre_carpeta = f"PV-{fecha_actual.strftime('%Y-%m-%d')}"
    carpeta_reporte = REPORTS_DIR / nombre_carpeta
    carpeta_reporte.mkdir(parents=True, exist_ok=True)
    return carpeta_reporte