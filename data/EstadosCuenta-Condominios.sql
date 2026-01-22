WITH EstadosCuentaCondominios AS (
      SELECT 
            id_cliente,	
            CONCAT (DESARROLLO, ' ', UNIDAD) AS Id,	
            MARCA AS Marca,	
            DESARROLLO AS Desarrollo,
            UNIDAD AS Unidad,	
            ETAPA AS Etapa,	
            CLIENTE AS Cliente,	
            CORREO AS Correo,	
            TELEFONO AS Telefono,	
            CONCAT("STP_", CLAVE_STP) AS Beneficiario_STP,	
            TOTALGENERADO AS Total_generado,	
            TOTAL_POR_COBRAR AS Total_por_cobrar,	
            CAST(SALDO_VENCIDO AS FLOAT64) AS Saldo_vencido,	
            DIAS_ATRASADO AS Dias_atraso,	
            SIGUIENTE_FECHA_PAGO AS Fecha_Proximo_pago,	
            DIA_PAGO AS Dia_pago,	
            CAST(TOTAL_COBRADO AS FLOAT64) AS Total_cobrado,	
            CAST(SALDOPENDIENTE_POR_APLICAR AS FLOAT64) AS Saldo_pendiente_por_aplicar,
            FECHA_ULTIMO_INGRESO AS Fecha_ultimo_ingreso 	
      FROM `terraviva-439415.sheets_condominios.sheets_estado_cuenta_condominios`
)
SELECT * FROM EstadosCuentaCondominios