import sys
import logging
from src.bigquery_client import BigQueryClient
from src.pipeline import procesar_bigquery_dataframe
from src.config import configurar_logger

logger = logging.getLogger(__name__)

def main() -> bool:
    """Función principal del pipeline de reportes"""
    configurar_logger()
    
    try:
        logger.info("=" * 60)
        logger.info("REPORTES DE POST VENTA - INICIANDO PIPELINE")
        logger.info("=" * 60)
        
        # 1. Extraer datos de BigQuery
        logger.info("1. Extrayendo datos de BigQuery")
        bq_client = BigQueryClient()
        dataframes_dict = bq_client.ejecutar_todas_consultas()
        
        if not dataframes_dict:
            logger.warning("No se obtuvieron datos de BigQuery")
            return False
        
        logger.info(f"✓ Se procesaron {len(dataframes_dict)} consultas exitosamente")
        
        # 2. Procesar y generar Excel
        logger.info("2. Generando archivos Excel con formato")
        resultados, carpeta_reporte = procesar_bigquery_dataframe(dataframes_dict)
        
        if resultados:
            logger.info("" + "=" * 60)
            logger.info("PIPELINE COMPLETADO EXITOSAMENTE")
            logger.info("=" * 60)
            logger.info(f"Archivos generados en: {carpeta_reporte}")
            for nombre, archivo in resultados.items():
                logger.info(f"  • {archivo.name}")
            logger.info(f"Total: {len(resultados)} archivos generados")
            return True
        else:
            logger.error("✗ ERROR: No se generaron archivos")
            return False
            
    except Exception as e:
        logger.error(f"✗ ERROR en el pipeline: {str(e)}", exc_info=True)
        return False

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)