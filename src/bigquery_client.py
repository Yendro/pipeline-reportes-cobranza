import pandas as pd
import logging
from google.cloud import bigquery
from google.oauth2 import service_account
from pathlib import Path
from typing import Dict, Optional
from src.config import BIGQUERY_CREDENTIALS_PATH, PROJECT_ID, obtener_consultas_sql

logger = logging.getLogger(__name__)

class BigQueryClient:
    """Cliente para ejecutar consultas en BigQuery"""
    
    def __init__(self):
        """Inicializar cliente de BigQuery"""
        self.credentials_path = Path(BIGQUERY_CREDENTIALS_PATH)
        self.project_id = PROJECT_ID
        self.client: Optional[bigquery.Client] = None
        
    def conectar(self) -> None:
        """Establecer conexión con BigQuery"""
        try:
            if not self.credentials_path.exists():
                raise FileNotFoundError(
                    f"Archivo de credenciales no encontrado: {self.credentials_path}"
                )
            
            credentials = service_account.Credentials.from_service_account_file(
                self.credentials_path,
                scopes=["https://www.googleapis.com/auth/cloud-platform"]
            )
            
            self.client = bigquery.Client(
                credentials=credentials,
                project=self.project_id
            )
            
            logger.info(f"Conectado a BigQuery - Proyecto: {self.project_id}")
            
        except Exception as e:
            logger.error(f"Error conectando a BigQuery: {str(e)}")
            raise
    
    def cargar_consulta_sql(self, sql_file: Path) -> str:
        """Cargar consulta SQL desde archivo (método interno)"""
        if not sql_file.exists():
            raise FileNotFoundError(f"Archivo SQL no encontrado: {sql_file}")
        
        with open(sql_file, 'r', encoding='utf-8') as f:
            sql_content = f.read()
        
        return sql_content
    
    def ejecutar_consulta(self, sql_file: Path) -> pd.DataFrame:
        """Ejecutar consulta y retornar como DataFrame"""
        if not self.client:
            self.conectar()
        
        sql = self.cargar_consulta_sql(sql_file)
        logger.info(f"Ejecutando consulta: {sql_file.name}")
        
        try:
            query_job = self.client.query(sql)
            results = query_job.result()
            df = results.to_dataframe()
            logger.info(f"Consulta completada. ({len(df)} filas, {len(df.columns)} columnas)")
            return df
        except Exception as e:
            logger.error(f"Error ejecutando consulta {sql_file}: {str(e)}")
            raise
    
    def ejecutar_todas_consultas(self) -> Dict[str, pd.DataFrame]:
        """Ejecutar todas las consultas SQL encontradas en data/"""
        try:
            if not self.client:
                self.conectar()
            
            archivos_sql = obtener_consultas_sql()
            
            if not archivos_sql:
                logger.warning("No se encontraron archivos SQL en data/")
                return {}
            
            logger.info(f"Encontrados {len(archivos_sql)} archivos SQL")
            resultados = {}
            
            for sql_file in archivos_sql:
                try:
                    nombre_consulta = sql_file.stem
                    df = self.ejecutar_consulta(sql_file)
                    resultados[nombre_consulta] = df
                    # logger.info(f"✓ {nombre_consulta}: {len(df)} filas")
                except Exception as e:
                    logger.error(f"✗ Error en {sql_file.name}: {str(e)}")
                    continue

            return resultados
            
        except Exception as e:
            logger.error(f"Error ejecutando todas las consultas: {str(e)}")
            raise