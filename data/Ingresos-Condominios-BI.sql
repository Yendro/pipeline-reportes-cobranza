WITH IngresosCondominiosBI AS (
    WITH temp_nombres_clientes AS (
        SELECT  
            id_cliente,
            id_manivela,
            id_venta_manviela,
            id_propiedad,
            TRIM(CONCAT( 
                ARRAY_TO_STRING(ARRAY(SELECT INITCAP(word) FROM UNNEST(SPLIT(TRIM(nombre), ' ')) AS word), ' '), ' ', 
                ARRAY_TO_STRING(ARRAY(SELECT INITCAP(word) FROM UNNEST(SPLIT(TRIM(REPLACE(REPLACE(apellido_p, '-', ''), '.', '')), ' ')) AS word), ' '), ' ', 
                ARRAY_TO_STRING(ARRAY(SELECT INITCAP(word) FROM UNNEST(SPLIT(TRIM(REPLACE(REPLACE(apellido_m, '-', ''), '.', '')), ' ')) AS word), ' ')))
            AS Cliente,
            nombre_nacionalidad,
            correo,
            telefono,
            PaisNombre,
            estadonombre,
        FROM EXTERNAL_QUERY("terraviva-439415.us.Condo", """
            SELECT
                c.id_cliente,
                c.id_manivela,
                c.id_venta_manviela,
                c.id_propiedad,
                c.nombre,
                c.apellido_p,
                c.apellido_m,
                c.nacionalidad,
                n.nacionalidad AS nombre_nacionalidad,
                p.PaisNombre,
                c.correo,
                e.estadonombre,
                c.telefono,
                c.id_pais,
                c.id_estado
            FROM cliente AS c
            LEFT JOIN nacionalidad AS n ON c.nacionalidad = n.id_nacionalidad
            LEFT JOIN Pais AS p ON c.id_pais = p.id_pais
            LEFT JOIN estado AS e ON c.id_estado = e.id
        """)
    ), 
    temp_propiedades AS (
        SELECT  
            id_propiedad,
            id_manivela,
            id_condominio,
            num_unidad,
            clave_stp,
            etapa,
            calle,
            num_interior,
            num_exterior,
            cruzamientos,
            colonia,
            status,
            tablaje
        FROM EXTERNAL_QUERY("terraviva-439415.us.Condo", """
            SELECT
                id_propiedad,
                id_manivela,
                id_condominio,
                num_unidad,
                clave_stp,
                etapa,
                calle,
                num_interior,
                num_exterior,
                cruzamientos,
                colonia,
                status,
                tablaje
            FROM propiedades
        """)
    ),
    temp_ingreso AS (
        SELECT  
            id_ingreso,
            id_cliente,
            id_usuario,
            id_forma_pago,
            id_banco,
            nombre_banco AS Banco,
            folio,
            monto AS Monto,
            tipo_cambio,
            monto_convertido,
            saldo_ingreso,
            status,
            NULLIF(fecha_ingreso, '0000-00-00') AS fecha_ingreso,
            NULLIF(fecha_creacion, '0000-00-00') AS fecha_creacion,
            NULLIF(fecha_cancelacion, '0000-00-00') AS fecha_cancelacion,
            status_notificacion,
            descripcion,
            comprobante,
            NULLIF(fecha_comprobante, '0000-00-00') AS fecha_comprobante,
            nombre AS FormaPago,
            cuentaBeneficiario,
        FROM EXTERNAL_QUERY("terraviva-439415.us.Condo", """
            SELECT
                i.id_ingreso,
                i.id_cliente,
                i.id_usuario,
                i.id_forma_pago,
                i.id_banco,
                i.folio,
                i.monto,
                i.tipo_cambio,
                NULLIF(i.fecha_ingreso, '0000-00-00') AS fecha_ingreso,
                NULLIF(i.fecha_comprobante, '0000-00-00') AS fecha_comprobante,
                NULLIF(i.fecha_creacion, '0000-00-00') AS fecha_creacion,
                NULLIF(i.fecha_cancelacion, '0000-00-00') AS fecha_cancelacion,
                i.monto_convertido,
                i.saldo_ingreso,
                i.status, 
                i.status_notificacion,
                i.descripcion,
                i.comprobante,
                b.nombre_banco,
                fp.nombre,
                stp.cuentaBeneficiario
            FROM ingreso AS i
            LEFT JOIN banco AS b ON i.id_banco = b.id_banco
            LEFT JOIN forma_pago AS fp ON i.id_forma_pago = fp.id_forma_pago
            LEFT JOIN stp_bitacora AS stp ON i.id_ingreso = stp.id_ingreso
            WHERE i.status = 1
        """)
    ),
    temp_flujo_detallado AS (
        SELECT  
            id_ingreso_dt,
            MONTO_CUOTA,
            MONTO_RESERVA,
            MONTO_FONDO
        FROM EXTERNAL_QUERY("terraviva-439415.us.Condo", """
            SELECT
                id_ingreso_dt,
                MONTO_CUOTA,
                MONTO_RESERVA,
                MONTO_FONDO
            FROM flujo_ingresos_detallado_sh
        """)
    ),
    temp_stp AS (
        SELECT  
            id_bitacora,
            id,
            fecha_Operacion,
            institucionOrdenante,
            institucionBeneficiaria,
            claveRastreo,
            monto,
            nombreOrdenante,
            tipoCuentaOrdenante,
            cuentaOrdenante,
            nombreBeneficiario,
            tipoCuentaBeneficiario,
            cuentaBeneficiario,
            nombreBeneficiario2,
            tipoCuentaBeneficiario2,
            cuentaBeneficiario2,
            rfcCurpBeneficiario,
            conceptoPago,
            referenciaNumerica,
            empresa,
            tipoPago,
            id_usuario_creacion,
            id_usuario_modificacion,
            id_usuario_cancelacion,
            NULLIF(fecha_creacion, '0000-00-00') AS fecha_creacion,
            NULLIF(fecha_modificacion, '0000-00-00') AS fecha_modificacion,
            NULLIF(fecha_cancelacion, '0000-00-00') AS fecha_cancelacion,
            status,
            id_ingreso, 
            status_identificacion,
            comentario_aplicacion,
            id_propiedad,
            id_condominio,  
            id_cliente,
            tipo_referencia
        FROM EXTERNAL_QUERY("terraviva-439415.us.Condo", """
            SELECT
                id_bitacora,
                id,
                fecha_Operacion,
                institucionOrdenante,
                institucionBeneficiaria,
                claveRastreo,
                monto,
                nombreOrdenante,
                tipoCuentaOrdenante,
                cuentaOrdenante,
                nombreBeneficiario,
                tipoCuentaBeneficiario,
                cuentaBeneficiario,
                nombreBeneficiario2,
                tipoCuentaBeneficiario2,
                cuentaBeneficiario2,
                rfcCurpBeneficiario,
                conceptoPago,
                referenciaNumerica,
                empresa,
                tipoPago,
                id_usuario_creacion,
                id_usuario_modificacion,
                id_usuario_cancelacion,
                NULLIF(fecha_creacion, '0000-00-00') AS fecha_creacion,
                NULLIF(fecha_modificacion, '0000-00-00') AS fecha_modificacion,
                NULLIF(fecha_cancelacion, '0000-00-00') AS fecha_cancelacion,
                status,
                id_ingreso, 
                status_identificacion,
                comentario_aplicacion, 
                id_propiedad,
                id_condominio,
                id_cliente,
                tipo_referencia
            FROM stp_bitacora
        """)
    ),
    temp_usuario AS(
        SELECT 
            id_usuario,
            TRIM(CONCAT( 
                ARRAY_TO_STRING(ARRAY(SELECT INITCAP(word) FROM UNNEST(SPLIT(TRIM(nombre), ' ')) AS word), ' '), ' ', 
                ARRAY_TO_STRING(ARRAY(SELECT INITCAP(word) FROM UNNEST(SPLIT(TRIM(REPLACE(REPLACE(apellido_paterno, '-', ''), '.', '')), ' ')) AS word), ' '), ' ', 
                ARRAY_TO_STRING(ARRAY(SELECT INITCAP(word) FROM UNNEST(SPLIT(TRIM(REPLACE(REPLACE(apellido_materno, '-', ''), '.', '')), ' ')) AS word), ' ')))
            AS Usuario, 
            correo_electronico,
            telefono,
            telefono_celular,
            contrasenia,
            id_perfil, 
            status,
            tipo,
            codigo,
            status_notificaciones,
            foto_usuario,
            clave_folio_usu,
            cambio_password,
            archivo_firma_digital
        FROM EXTERNAL_QUERY("terraviva-439415.us.Condo", """
            SELECT
                id_usuario,
                nombre,
                apellido_paterno,
                apellido_materno,
                correo_electronico,
                telefono,
                telefono_celular,
                contrasenia,
                id_perfil,
                status,
                tipo,
                codigo,
                status_notificaciones,
                foto_usuario,
                clave_folio_usu,
                cambio_password,
                archivo_firma_digital
            FROM usuario
        """)
    ),
    temp_condominio AS (
        SELECT  
            id_condominio,
            id_sistema,
            id_manivela,
            nombre_condominio,
            nombre_condominio_corto,
            id_almacen,
            clave_folio_ingresos,
            numero_folio_ingresos,
            direccion_condominio,
            estado,
            localidad,
            pais,
            municipio,
            id_tipo_moneda,
            status,
            codigo_postal,
            id_usuario_creacion,
            id_usuario_modificacion,
            id_usuario_cancelacion,
            codigo_oculta,
            municipio_localidad,
            numero_total_unidades
        FROM EXTERNAL_QUERY("terraviva-439415.us.Condo", """
            SELECT
                id_condominio,
                id_sistema,
                id_manivela,
                nombre_condominio,
                nombre_condominio_corto,
                id_almacen,
                clave_folio_ingresos,
                numero_folio_ingresos,
                direccion_condominio,
                estado,
                localidad,
                pais,
                municipio,
                id_tipo_moneda,
                status,
                codigo_postal,
                id_usuario_creacion,
                id_usuario_modificacion,
                id_usuario_cancelacion,
                codigo_oculta,
                municipio_localidad,
                numero_total_unidades
            FROM condominio
        """) 
    ) 
    SELECT 
        ti.id_ingreso,
        tnc.id_propiedad AS id_venta,
        nd.Marca,
        nd.Desarrollo,
        nd.Privada,
        tp.etapa AS Etapa,
        tp.num_unidad AS Unidad,
        ti.folio,
        tnc.Cliente, 
        tnc.id_cliente,
        tu.Usuario,
        ti.cuentaBeneficiario,
        ti.fecha_ingreso AS Fecha,
        ti.Banco,
        ti.FormaPago,
        ti.Monto,
        tfd.MONTO_CUOTA AS MontoCuota,
        tfd.MONTO_RESERVA AS MontoReserva,
        tfd.MONTO_FONDO AS MontoFondo
    FROM temp_ingreso AS ti
    LEFT JOIN temp_nombres_clientes AS tnc ON ti.id_cliente = tnc.id_cliente
    LEFT JOIN temp_propiedades AS tp ON tnc.id_propiedad = tp.id_propiedad
    LEFT JOIN temp_condominio AS tc ON tp.id_condominio = tc.id_condominio
    LEFT JOIN `Dimensiones.NombreDesarrollo` AS nd ON tc.nombre_condominio = nd.id_nombre_desarrollo
    LEFT JOIN temp_usuario AS tu ON ti.id_usuario = tu.id_usuario
    LEFT JOIN temp_flujo_detallado AS tfd ON ti.id_ingreso = tfd.id_ingreso_dt
)
SELECT
    id_ingreso,
    id_venta,
    Marca,
    Desarrollo,
    Privada,
    Etapa,
    Unidad,
    folio,
    Cliente, 
    Usuario,
    CONCAT ("STP_", cuentaBeneficiario) AS cuentaBeneficiario,
    DATE(Fecha) AS FechaPago,
    Banco,
    FormaPago,
    Monto,
    CAST(
        CASE 
            WHEN MontoCuota IS NULL AND MontoReserva IS NULL AND MontoFondo IS NULL THEN 0
            ELSE COALESCE(MontoCuota, 0)
        END AS FLOAT64
    ) AS MontoCuota,
    CAST(
        CASE 
            WHEN MontoCuota IS NULL AND MontoReserva IS NULL AND MontoFondo IS NULL THEN 0
            ELSE COALESCE(MontoReserva, 0)
        END AS FLOAT64
    ) AS MontoReserva,
    CAST(
        CASE 
            WHEN MontoCuota IS NULL AND MontoReserva IS NULL AND MontoFondo IS NULL THEN 0
            ELSE COALESCE(MontoFondo, 0)
        END AS FLOAT64
    ) AS MontoFondo,
    -- FONDOS FUTUROS (SIN ESTADOS DE CUENTA)
    CAST(    
        CASE
            WHEN MontoCuota IS NULL AND MontoReserva IS NULL AND MontoFondo IS NULL THEN Monto
            ELSE 0
        END AS FLOAT64
    ) AS FondosFuturos
FROM IngresosCondominiosBI
WHERE Desarrollo IS NOT NULL
    AND Desarrollo != 'Demo'
    AND Usuario != 'Super Bq Administrador Manivela'
    AND FormaPago IS NOT NULL
    AND NOT (Desarrollo = 'San Eduardo' AND Privada = 'P5')
    AND Banco NOT IN ('GLR', 'REDONDEO')
    AND EXTRACT (YEAR FROM DATE(Fecha)) = EXTRACT(YEAR FROM CURRENT_DATE())
    AND EXTRACT (MONTH FROM DATE(Fecha))= EXTRACT(MONTH FROM CURRENT_DATE()) 