import pandas as pd
import logging
from datetime import datetime
from numbers import Number
from pathlib import Path
from openpyxl import load_workbook
from openpyxl.styles import PatternFill, Font, Alignment, Border, Side
from src.config import crear_carpeta_reporte, obtener_archivo_de_salida

logger = logging.getLogger(__name__)

# Configuración de formatos para encabezados
FORMATO_ENCABEZADO = {
    "fill": PatternFill(start_color="0070C0", end_color="0070C0", fill_type="solid"),
    "font": Font(bold=True, color="FFFFFF", size=11),
    "alignment": Alignment(horizontal="center", vertical="center", wrap_text=True),
    "border": Border(
        left=Side(style='thin'),
        right=Side(style='thin'),
        top=Side(style='thin'),
        bottom=Side(style='thin')
    )
}

CATEGORIAS_FORMATO_EXCEL = {
    "fecha": {
        "nombre_columna": [
            # Ingresos-GAIA
            "fecha_ingreso", "fecha_creacion", "fecha_ultimo_ingreso", 

            # EstadosCuenta-GAIA
            "Fecha_Contrato", "Fecha_Firma_Contrato", "Fecha_Proximo_pago", "fecha_promesa",

            # Ingresos-Condominios
            "FECHA_INGRESO", "FECHA_REGISTRO",

            # Ingresos-Condominios-BI
            "FechaPago",

            # EstadosCuenta-Condominios
            # "Fecha_Proximo_pago",
            "Fecha_ultimo_ingreso",
            
            # CarteraVencida-Condominios
            "FECHA_NACIMIENTO", "FECHAPAGO",
        ],
        "formato_excel": "DD/MM/YYYY",
    },
    # "fecha_hora": {
    #     "nombre_columna": [
    #         "timestamp", "fecha_hora", "datetime"
    #     ],
    #     "formato_excel": "DD/MM/YYYY HH:MM",
    # },
    "moneda": {
        "nombre_columna": [
            # Ingresos-GAIA
            "Cantidad", "Gastos_gestion",

            # EstadosCuenta-GAIA
            "Precio_venta", "Total_cobrado", "Enganche_pagado", "Total_por_cobrar", "Mensualidad", "Total_Requerido", "Cobrado", "Acumulado_Vencido", "Monto_ultimo_ingreso", "Acumado_Vencido",

            # Ingresos-Condominios
            "MONTO_PAGADO", "SALDO_PENDIENTE_POR_APLICAR", "MONTO_CUOTA", "MONTO_RESERVA", "MONTO_FONDO",

            # Ingresos-Condominios-BI
            "Monto", "MontoCuota", "MontoReserva", "MontoFondo", "FondosFuturos",

            # EstadosCuenta-Condominios
            # "Total_por_cobrar", "Total_cobrado",,m
            "Total_generado", "Saldo_vencido", "Saldo_pendiente_por_aplicar",
            
            # CarteraVencida-Condominios
            "TOTAL_PAGO", "SALDO_VENCIDO",
        ],
        "formato_excel": '"$"#,##0.00',
    },
    "numero": {
        "nombre_columna": [
            # Ingresos-GAIA
            "id_venta", "id_ingreso",

            # EstadosCuenta-GAIA
            # "id_venta",
            "dia_pago", "Meses_Financia", "Dias_Atrasado", "numero_pago",

            # Ingresos-Condominios
            "IDINGRESO", "IDCLIENTE", "NUMERO_CUENTA", "id_ingreso_dt",

            # Ingresos-Condominios-BI
            #"id_ingreso", "id_venta",

            # EstadosCuenta-Condominios
            "id_cliente", "Dias_atraso", "Dia_pago",
            
            # CarteraVencida-Condominios
            "IDCLIENTE", "DIA_VENCIDO",
        ],
        "formato_excel": '0',
    },
    # "numero_decimal": {
    #     "nombre_columna": [
    #         "metros", "unidades", "numero", "count", "cantidad_total",
    #     ],
    #     "formato_excel": '#,##0',
    # },
    "texto": {
        "nombre_columna": [
            # Ingresos-GAIA
            "id", "Marca", "Desarrollo", "Privada", "Etapa", "Unidad", "Folio", "Cliente", "STP", "Estatus", "forma_de_pago", "concepto", "flujo_concepto", "Banco", "Usuario_asignacion",

            # EstadosCuenta-GAIA
            # "id", "Marca", "Desarrollo", "Privada", "Etapa", "Unidad", "Cliente", "Estatus",
            "Copropietario", "Asesor", "Sucursal", "Tipo", "Equipo", "telefono_celular", "correo_electronico", "CuentaBeneficiarioReal",

            # Ingresos-Condominios
            # "STP",
            "DESARROLLO", "UNIDAD", "FOLIO", "CLIENTE", "BANCO", "FORMA_PAGO", "USUARIOS_REGISTRO", "STATUS", "SISTEMA",

            # Ingresos-Condominios-BI
            # "Marca", "Desarrollo", "Privada", "Etapa", "Unidad", "Cliente", "Banco",
            "folio", "Usuario", "cuentaBeneficiario", "FormaPago",

            # EstadosCuenta-Condominios
            # "id", "Marca", "Desarrollo", "Unidad", "Etapa", "Cliente",
            "Correo", "Telefono", "Beneficiario_STP",
            
            # CarteraVencida-Condominios
            # "DESARROLLO", "UNIDAD", "CLIENTE", "SISTEMA",
            "CORREO", "TELEFONO", "nombre",
        ],
        "formato_excel": '@',
    },
}

def detectar_categoria_columna(nombre_columna):
    """Detecta la categoría de una columna basada en palabras clave"""
    if not nombre_columna:
        return None
    
    nombre = str(nombre_columna).lower().strip()
    
    for categoria, config in CATEGORIAS_FORMATO_EXCEL.items():
        for keyword in config["nombre_columna"]:
            # Busca coincidencias insensibles a mayúsculas/minúsculas
            if keyword.lower() in nombre:
                return categoria
    
    return None

def obtener_formato_para_columna(nombre_columna):
    """
    Busca el formato apropiado para una columna basado en su nombre.
    
    Args:
        nombre_columna (str): Nombre de la columna
    
    Returns:
        str: Formato de Excel o None si no hay coincidencia
    """
    categoria = detectar_categoria_columna(nombre_columna)

    if categoria:
        return CATEGORIAS_FORMATO_EXCEL[categoria]["formato_excel"]

    return None

def normalizar_df_para_excel(df):
    """
    Normaliza tipos de datos para que Excel aplique formatos correctamente.
    """
    df = df.copy()

    # Verificar el tipo de dato que se va a aplicar el formato
    # print(df[col].head())
    # print(df[col].apply(type).value_counts())
    
    for col in df.columns:
        categoria = detectar_categoria_columna(col)
        
        if categoria == "fecha":
            try:
                # Convertir a datetime, manejando diferentes formatos
                df[col] = pd.to_datetime(df[col], errors="coerce")
                # Remover zona horaria si existe
                if hasattr(df[col], 'dt'):
                    df[col] = df[col].dt.tz_localize(None)
            except Exception:
                pass
                
        elif categoria == "moneda":
            try:
                # Limpiar caracteres no numéricos como $, , (comas), espacios
                if df[col].dtype == 'object':
                    # Remover símbolos de moneda, comas y espacios
                    df[col] = df[col].astype(str).str.replace(r'[^\d\.\-]', '', regex=True)
                    # Reemplazar múltiples puntos por uno solo (para decimales)
                    df[col] = df[col].str.replace(r'\.+', '.', regex=True)
                
                # Convertir a numérico
                df[col] = pd.to_numeric(df[col], errors="coerce")
                
                # Redondear a 2 decimales para moneda
                df[col] = df[col].round(2)
                
            except Exception as e:
                logger.warning(f"No se pudo convertir columna {col} a moneda: {str(e)}")
                
        elif categoria == "numero":
            df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0).astype('Int64')
            
        elif categoria == "numero_decimal":
            df[col] = pd.to_numeric(df[col], errors="coerce").round(2)
            
        elif categoria == "texto":
            df[col] = df[col].fillna('').astype(str).str.strip()
    
    return df

def crear_excel_con_formato(df, output_path, nombre_consulta):
    """
    Crea un archivo Excel con formato aplicado automáticamente.
    
    Args:
        df: DataFrame de pandas
        output_path: Ruta donde guardar el archivo
    
    Returns:
        bool: True si se creó correctamente
    """
    try:
        # Guardar DataFrame como Excel
        df = normalizar_df_para_excel(df)

        with pd.ExcelWriter(output_path, engine='openpyxl') as writer:
            df.to_excel(writer, index=False, sheet_name=f'{nombre_consulta}')
        
        # Aplicar formato al archivo guardado
        return aplicar_formato_excel(output_path)
        
    except Exception as e:
        logger.error(f"Error creando Excel con formato: {str(e)}")
        return False

def aplicar_formato_excel(archivo_excel):
    """
    Aplica formato profesional a un archivo Excel.
    """
    try:
        workbook = load_workbook(archivo_excel)
        worksheet = workbook.active
        
        # Aplicar formato a encabezados
        aplicar_formato_encabezados(worksheet)
        
        # Ajustar ancho de columnas
        ajustar_ancho_columnas(worksheet)
        
        # Aplicar formatos específicos a datos
        aplicar_formatos_datos_normalizado(worksheet)
        
        # Guardar cambios
        workbook.save(archivo_excel)
        logger.debug(f"Formato aplicado a: {archivo_excel.name}")
        return True
        
    except Exception as e:
        logger.error(f"Error en aplicar_formato_excel: {str(e)}")
        return False

def aplicar_formato_encabezados(worksheet):
    """Aplica formato a la fila de encabezados"""
    if worksheet.max_row == 0 or worksheet.max_column == 0:
        return
    
    for col in range(1, worksheet.max_column + 1):
        cell = worksheet.cell(row=1, column=col)
        
        # Aplicar estilos
        cell.fill = FORMATO_ENCABEZADO["fill"]
        cell.font = FORMATO_ENCABEZADO["font"]
        cell.alignment = FORMATO_ENCABEZADO["alignment"]
        cell.border = FORMATO_ENCABEZADO["border"]
        
        # # Opcional: convertir a mayúsculas
        # if cell.value:
        #     cell.value = str(cell.value).strip().upper()

def ajustar_ancho_columnas(worksheet, max_width=50, min_width=8):
    """Ajusta automáticamente el ancho de las columnas"""
    for column_cells in worksheet.columns:
        column_letter = column_cells[0].column_letter
        max_length = 0
        
        # Encontrar la longitud máxima del contenido
        for cell in column_cells:
            try:
                if cell.value:
                    cell_length = len(str(cell.value))
                    if cell_length > max_length:
                        max_length = cell_length
            except:
                pass
        
        # Calcular ancho ajustado
        adjusted_width = (max_length + 2) * 1.1
        adjusted_width = max(min_width, min(adjusted_width, max_width))
        
        worksheet.column_dimensions[column_letter].width = adjusted_width

# def aplicar_formatos_datos_normalizado(worksheet):
#     """
#     Aplica formatos específicos a las columnas de datos usando el mapeo normalizado.
#     """
#     if worksheet.max_row < 2 or worksheet.max_column == 0:
#         return
    
#     # Crear diccionario de encabezados con su índice de columna
#     encabezados = {}
#     for col in range(1, worksheet.max_column + 1):
#         header_cell = worksheet.cell(row=1, column=col)
#         if header_cell.value:
#             encabezados[col] = str(header_cell.value).strip()
    
#     # Para cada columna, buscar y aplicar formato
#     columnas_formateadas = 0
#     for col_idx, nombre_columna in encabezados.items():
#         formato = obtener_formato_para_columna(nombre_columna)
        
#         if formato:
#             # Aplicar formato a todas las celdas de esta columna (excepto encabezado)
#             for row in range(2, worksheet.max_row + 1):
#                 cell = worksheet.cell(row=row, column=col_idx)
#                 if isinstance(cell.value, (Number, datetime)):
#                     cell.number_format = formato
#             columnas_formateadas += 1
    
#     logger.info(f"Formato aplicado a {columnas_formateadas} de {len(encabezados)} columnas")

def aplicar_formatos_datos_normalizado(worksheet):
    """
    Aplica formatos específicos a las columnas de datos usando el mapeo normalizado.
    """
    if worksheet.max_row < 2 or worksheet.max_column == 0:
        return
    
    # Crear diccionario de encabezados con su índice de columna
    encabezados = {}
    for col in range(1, worksheet.max_column + 1):
        header_cell = worksheet.cell(row=1, column=col)
        if header_cell.value:
            encabezados[col] = str(header_cell.value).strip()
    
    # Para cada columna, buscar y aplicar formato
    columnas_formateadas = 0
    for col_idx, nombre_columna in encabezados.items():
        formato = obtener_formato_para_columna(nombre_columna)
        
        if formato:
            # Aplicar formato a todas las celdas de esta columna (excepto encabezado)
            for row in range(2, worksheet.max_row + 1):
                cell = worksheet.cell(row=row, column=col_idx)
                
                # Aplicar formato según el tipo de dato
                if cell.value is not None:
                    # Para moneda, asegurar que sea número
                    if "$" in formato and isinstance(cell.value, (int, float)):
                        cell.number_format = formato
                        # También puedes forzar 2 decimales si es necesario
                        if isinstance(cell.value, float):
                            cell.value = round(cell.value, 2)
                    else:
                        cell.number_format = formato
            
            columnas_formateadas += 1
            logger.debug(f"Aplicado formato {formato} a columna {nombre_columna}")
    
    logger.info(f"Formato aplicado a {columnas_formateadas}/{len(encabezados)} columnas")

def procesar_bigquery_dataframe(dataframes_dict):
    """
    Procesar múltiples DataFrames y guardar cada uno en archivo Excel con formato.
    """
    # Crear carpeta de reporte
    carpeta_reporte = crear_carpeta_reporte()
    logger.info(f"Carpeta de reporte: {carpeta_reporte}")
     
    resultados = {}
    
    for nombre_consulta, df in dataframes_dict.items():
        if df.empty:
            logger.warning(f"DataFrame vacío para {nombre_consulta}")
            continue
        
        try:
            # Generar nombre de archivo
            nombre_archivo = obtener_archivo_de_salida(nombre_consulta)
            output_path = carpeta_reporte / nombre_archivo
            
            logger.info(f"Procesando: {nombre_consulta} ({len(df.columns)} columnas)")
            # logger.info(f"Procesando: {nombre_consulta} ({len(df)} filas, {len(df.columns)} columnas)")
            
            # Crear Excel con formato
            if crear_excel_con_formato(df, output_path, nombre_consulta):
                resultados[nombre_consulta] = output_path
                logger.info(f"  ✓ Guardado con formato: {output_path.name}")
            else:
                logger.warning(f"  ⚠ Guardado sin formato: {output_path.name}")
                resultados[nombre_consulta] = output_path
                
        except Exception as e:
            logger.error(f"Error procesando {nombre_consulta}: {str(e)}")
    
    return resultados, carpeta_reporte