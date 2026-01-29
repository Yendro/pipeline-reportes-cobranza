import sys
import logging
from pathlib import Path
from src.bigquery_client import BigQueryClient
from src.pipeline import procesar_bigquery_dataframe
from src.config import configurar_logger

def main():
    """Función principal del pipeline"""
    configurar_logger()
    logger = logging.getLogger(__name__)
    
    try:
        logger.info("===== REPORTES DE POST VENTA =====")
        
        logger.info("Extrayendo datos de BigQuery de todas las consultas")
        bq_client = BigQueryClient()
        dataframes_dict = bq_client.ejecutar_todas_consultas()
        
        if not dataframes_dict:
            logger.warning("No se obtuvieron datos de BigQuery")
            return False
        
        logger.info("="*60)
        logger.info(f"Se procesaron {len(dataframes_dict)} consultas exitosamente")
        
        # Mostrar estadísticas de cada consulta
        for nombre, df in dataframes_dict.items():
            logger.info(f"Datos en {nombre}: {len(df)} filas, {len(df.columns)} columnas")

        logger.info("="*60)
        logger.info("Transformando datos y generando archivos Excel")
        resultados, carpeta_reporte = procesar_bigquery_dataframe(dataframes_dict)
        
        if resultados:
            logger.info(f"===== PIPELINE COMPLETADO EXITOSAMENTE =====")
            for nombre, archivo in resultados.items():
                logger.info(f"  ✓ {nombre}: {archivo.name}")

            logger.info(f"Total archivos generados: {len(resultados)}")
            return True
        else:
            logger.error("=== ERROR: No se generaron archivos ===")
            return False
            
    except Exception as e:
        logger.error(f"Error en el pipeline principal: {str(e)}", exc_info=True)
        return False

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)