import pandas as pd
from google.cloud import bigquery
from google.oauth2 import service_account
import logging
from pathlib import Path
from src.config import BIGQUERY_CREDENTIALS_PATH, PROJECT_ID, obtener_consultas_sql

logger = logging.getLogger(__name__)

class BigQueryClient:
    def __init__(self):
        """Inicializar cliente de BigQuery"""
        self.credentials_path = Path(BIGQUERY_CREDENTIALS_PATH)
        self.project_id = PROJECT_ID
        self.client = None
        
    def conectar(self):
        """Establecer conexión con BigQuery"""
        try:
            if not self.credentials_path.exists():
                raise FileNotFoundError(
                    f"Archivo de credenciales no encontrado: {self.credentials_path}"
                )
            
            credentials = service_account.Credentials.from_service_account_file(
                self.credentials_path,
                scopes = ["https://www.googleapis.com/auth/cloud-platform"]
            )
            
            self.client = bigquery.Client(
                credentials = credentials,
                project = self.project_id
            )
            
            logger.info(f"Conectado a BigQuery - Proyecto: {self.project_id}")
            return True
            
        except Exception as e:
            logger.error(f"Error conectando a BigQuery: {str(e)}")
            raise
    
    def cargar_consulta_sql(self, sql_file):
        """Cargar consulta SQL desde archivo"""
        
        if not sql_file.exists():
            raise FileNotFoundError(f"Archivo SQL no encontrado: {sql_file}")
        
        with open(sql_file, 'r', encoding='utf-8') as f:
            sql_content = f.read()
        
        return sql_content
    
    def ejecutar_consulta(self, sql_file):
        """Ejecutar consulta y retornar como DataFrame"""
        try:
            if not self.client:
                self.conectar()

            sql = self.cargar_consulta_sql(sql_file)
            logger.info(f"Ejecutando consulta: {sql_file.name}")

            query_job = self.client.query(sql)
            results = query_job.result()

            df = results.to_dataframe()
            logger.info(f"Consulta completada.")
            return df
            
        except Exception as e:
            logger.error(f"Error ejecutando consulta {sql_file}: {str(e)}")
            raise
    
    def ejecutar_todas_consultas(self):
        """Ejecutar todas las consultas SQL encontradas en data/"""
        try:
            if not self.client:
                self.conectar()
            
            archivos_sql = obtener_consultas_sql()
            
            if not archivos_sql:
                logger.warning("No se encontraron archivos SQL en data/")
                return {}
            
            logger.info(f"Encontrados {len(archivos_sql)} archivos SQL")
            logger.info("="*60)
            resultados = {}
            
            for sql_file in archivos_sql:
                try:
                    # Nombre base del archivo (sin extensión)
                    nombre_consulta = sql_file.stem
                    
                    df = self.ejecutar_consulta(sql_file)
                    resultados[nombre_consulta] = df
                    
                except Exception as e:
                    logger.error(f"Error en consulta {sql_file.name}: {str(e)}")
                    # Continuar con las siguientes consultas
                    continue

            return resultados
            
        except Exception as e:
            logger.error(f"Error ejecutando todas las consultas: {str(e)}")
            raise