--Fix: Se cambio el origen de la clave stp para que no hayan datos nulos.
--Refactor: Se modificaron las subconsultas para concatenar a los coopropietarios en un solo campo y evitar duplicados.
--Feat: Se añadio el campo de etapa al reporte.
--Refactor: Se modifico la condicional de id_desarrollo para tener la etapa.

WITH Ventas3Version AS (
    WITH VentasConsolidades AS (
    --CUSTO
        SELECT
            id_venta, 
            'Custo' AS Origen,
            nombre_desarrollo AS Desarrollo,
            privada AS Privada,  
            numero_etapa AS Etapa,
            numero_unidad AS Unidad,
            NombreAsesor, ApellidoPaternoAsesor, ApellidoMaternoAsesor,
            NombreCliente, ApellidoPCliente, ApellidoMCliente, 
            CopropietariosConcatenados AS NombreCoproConcatenado,
            telefono_celular,
            correo_electronico, 
            cuentaBeneficiario, 
            nombre_status, 
            referencia_banco,
            Fecha_Contrato,
            Aprobado_juridico AS Fecha_Firma_Contrato,
            precio_venta,  
            dia_pago,
            total_pagado AS Total_cobrado,
            cantidad_enganche,
            saldo_total AS total_por_cobrar,
            (numero_pagos) + 1 AS Meses_Financia,
            Mensualidad,
            DATE_DIFF(CURRENT_DATE(), primera_fecha_vencido, DAY) AS dias_en_mora,
            saldo_vencido_total,
            total_requerido,
            pago_vencido_total,
            numero_pago,
            DATE(fecha_ultimo_ingreso) AS fecha_ultimo_ingreso,
            monto_ultimo_ingreso,
            monto_ultimo_ingreso_cobrado,
            siguiente_fecha_pago,
            DATE(fecha_promesa) AS fecha_promesa
        FROM EXTERNAL_QUERY("terraviva-439415.us.custo",
          """SELECT 
                  v.id_venta, 
                    NULLIF(v.fecha_cierre_venta, '0000-00-00') AS Fecha_Contrato,
                    v.precio_venta,   
                    v.total_pagado,
                    v.cantidad_enganche, 
                    v.saldo_total,
                    v.numero_pagos,
                    u.nombre AS NombreAsesor, 
                    u.apellido_paterno AS ApellidoPaternoAsesor, 
                    u.apellido_materno AS ApellidoMaternoAsesor,
                    c.nombre AS NombreCliente, 
                    c.apellido_p AS ApellidoPCliente, 
                    c.apellido_m AS ApellidoMCliente,

                    cop.CopropietariosConcatenados,

                    c.telefono_celular,
                    c.correo_electronico,
                    d.nombre_desarrollo, 
                    uni.privada, 
                    uni.numero_etapa,
                    uni.numero_unidad, 
                    uni.modelo,   
                    uni.referencia_banco,
                    sv.nombre AS nombre_status, 
                    DATE(i.fecha_ultimo_ingreso) AS fecha_ultimo_ingreso,
                    i.monto_ultimo_ingreso,
                    monto_ultimo_ingreso_cobrado,
                    stp.cuentaBeneficiario,
                    tp.dia_pago,
                    tp.Mensualidad,
                  bv.Aprobado_juridico,  
                  venc.primera_fecha_vencido,
                  venc.saldo_vencido_total,
                  venc.total_requerido,
                  venc.pago_vencido_total,
                  venc.numero_pago,
                  DATE(venc.siguiente_fecha_pago) AS siguiente_fecha_pago,
                  DATE(fp.fecha_promesa) AS fecha_promesa

                             
           FROM venta AS v
           LEFT JOIN (
                        SELECT 
                           i.id_venta,
                           DATE(NULLIF(i.fecha_ingreso, '0000-00-00')) AS fecha_ultimo_ingreso,
                           i.monto_ingresado AS monto_ultimo_ingreso
                        FROM ingreso i
                        INNER JOIN (
                           SELECT 
                               id_venta,
                               MAX(id_ingreso) as max_id
                           FROM ingreso
                           WHERE fecha_aprobacion IS NOT NULL
                           GROUP BY id_venta
                        ) ultimo ON i.id_ingreso = ultimo.max_id
                        WHERE i.fecha_aprobacion IS NOT NULL
                        
                        ) AS i ON v.id_venta = i.id_venta 
           LEFT JOIN (
                         SELECT 
                             i.id_venta,
                             DATE(NULLIF(i.fecha_ingreso, '0000-00-00')) AS fecha_ultimo_ingreso,
                             CASE 
                                 WHEN EXTRACT(YEAR FROM DATE(NULLIF(i.fecha_ingreso, '0000-00-00'))) = EXTRACT(YEAR FROM CURRENT_DATE())
                                 AND EXTRACT(MONTH FROM DATE(NULLIF(i.fecha_ingreso, '0000-00-00'))) = EXTRACT(MONTH FROM CURRENT_DATE())
                                 THEN i.monto_ingresado
                                 ELSE 0
                             END AS monto_ultimo_ingreso_cobrado
                         FROM ingreso i
                         INNER JOIN (
                             SELECT 
                                 id_venta,
                                 MAX(id_ingreso) as max_id
                             FROM ingreso
                             WHERE fecha_aprobacion IS NOT NULL
                             GROUP BY id_venta
                         ) ultimo ON i.id_ingreso = ultimo.max_id
                         WHERE i.fecha_aprobacion IS NOT NULL
                     ) AS ia ON v.id_venta = ia.id_venta
           LEFT JOIN (SELECT id_venta, MAX(DATE(NULLIF(fecha_promesa, '0000-00-00'))) AS fecha_promesa FROM seguimiento GROUP BY id_venta) AS fp ON v.id_venta = fp.id_venta
           LEFT JOIN (SELECT id_venta, cuentaBeneficiario FROM stp_bitacora WHERE status = 1 GROUP BY id_venta) AS stp ON v.id_venta = stp.id_venta
           LEFT JOIN usuario AS u ON v.id_usuario = u.id_usuario 
           LEFT JOIN cliente AS c ON v.id_cliente = c.id_cliente
           LEFT JOIN (
            SELECT 
                id_venta,
                GROUP_CONCAT(
                    CASE 
                        -- Si todos los campos están vacíos, omitir
                        WHEN TRIM(CONCAT(
                            COALESCE(nombre, ''),
                            COALESCE(apellido_p, ''),
                            COALESCE(apellido_m, '')
                        )) = '' THEN NULL
                        -- Construir nombre completo sin espacios extra
                        ELSE TRIM(
                            CONCAT_WS(' ',
                                NULLIF(TRIM(nombre), ''),
                                NULLIF(TRIM(apellido_p), ''),
                                NULLIF(TRIM(apellido_m), '')
                            )
                        )
                    END SEPARATOR ', '
                ) AS CopropietariosConcatenados
            FROM copropietario 
            GROUP BY id_venta
            HAVING GROUP_CONCAT(
                TRIM(CONCAT(
                    COALESCE(nombre, ''),
                    COALESCE(apellido_p, ''),
                    COALESCE(apellido_m, '')
                ))
            ) IS NOT NULL
            ) AS cop ON v.id_venta = cop.id_venta
           LEFT JOIN unidades AS uni ON v.id_unidad = uni.id_unidad LEFT JOIN desarrollo AS d ON uni.id_desarrollo = d.id_desarrollo
           LEFT JOIN status_venta AS sv ON v.status_venta = sv.id_status
           LEFT JOIN (
                        SELECT 
                            tap.id_venta, 
                            MAX(EXTRACT(DAY FROM  fecha_pago )) AS dia_pago, 
                            pago_total AS Mensualidad
                        FROM tabla_pagos AS tap
                        LEFT JOIN tipo_pago AS tip ON tap.id_tipo_pago = tip.id_tipo_pago
                        WHERE tip.nombre_tipo_pago = 'Mensualidad'
                        GROUP BY tap.id_venta
                      ) AS tp ON v.id_venta = tp.id_venta
           LEFT JOIN (
                        SELECT 
                            tap.id_venta,
                                 MIN(CASE  WHEN tap.saldo_pago_total != 0  AND DATE(tap.fecha_pago) < CURRENT_DATE()  THEN DATE(tap.fecha_pago)  END) AS primera_fecha_vencido,
                                 SUM(CASE WHEN tap.saldo_pago_total != 0  AND DATE(tap.fecha_pago) < CURRENT_DATE() THEN tap.saldo_pago_total ELSE 0 END) AS saldo_vencido_total,
                                 SUM(CASE WHEN tap.saldo_pago_total != 0 AND DATE(tap.fecha_pago) < CURRENT_DATE() THEN tap.saldo_pago_total ELSE 0 END) +
                                 COALESCE(
                                     (SELECT tap2.saldo_pago_total 
                                      FROM tabla_pagos tap2 
                                      WHERE tap2.id_venta = tap.id_venta 
                                        AND tap2.saldo_pago_total != 0 
                                        AND DATE(tap2.fecha_pago) >= CURRENT_DATE()
                                      ORDER BY tap2.fecha_pago ASC 
                                      LIMIT 1),
                                     0
                                 ) AS total_requerido,
                                 SUM(CASE WHEN tap.saldo_pago_total != 0  AND DATE(tap.fecha_pago) < CURRENT_DATE() THEN tap.pago_total ELSE 0 END) AS pago_vencido_total,
                                 MIN(CASE WHEN tap.saldo_pago_total != 0 AND DATE(tap.fecha_pago) >= CURRENT_DATE() THEN DATE(tap.fecha_pago) END) AS siguiente_fecha_pago,
                                 MIN(CASE WHEN tap.saldo_pago_total != 0 AND DATE(tap.fecha_pago) >= CURRENT_DATE() THEN numero_pago END) AS numero_pago
                        FROM tabla_pagos AS tap
                        LEFT JOIN tipo_pago AS tip 
                               ON tap.id_tipo_pago = tip.id_tipo_pago 
                        GROUP BY tap.id_venta
                    ) AS venc ON v.id_venta = venc.id_venta
           LEFT JOIN (
                       SELECT
                           bv.id_venta,
                           MIN(CASE WHEN sv2.nombre LIKE 'Aprobado jur%' THEN DATE(bv.fecha_movimiento) END) AS Aprobado_juridico
                       FROM `bitacora_venta` AS bv
                       LEFT JOIN status_venta sv2 ON bv.id_status_venta = sv2.id_status
                       GROUP BY bv.id_venta
                     ) AS bv ON v.id_venta = bv.id_venta

           WHERE v.status_venta IN (7, 8)""")
            GROUP BY id_venta, Origen, nombre_desarrollo, privada, numero_etapa, numero_unidad,  NombreAsesor, ApellidoPaternoAsesor, ApellidoMaternoAsesor, NombreCliente, ApellidoPCliente, ApellidoMCliente,  CopropietariosConcatenados, telefono_celular, correo_electronico,  cuentaBeneficiario,  nombre_status,  referencia_banco, Fecha_Contrato, Aprobado_juridico, precio_venta, dia_pago, total_pagado, cantidad_enganche, saldo_total, numero_pagos, Mensualidad, primera_fecha_vencido, saldo_vencido_total, total_requerido, pago_vencido_total, numero_pago, fecha_ultimo_ingreso, monto_ultimo_ingreso, monto_ultimo_ingreso_cobrado, siguiente_fecha_pago, fecha_promesa
    UNION ALL

    --DAM
        SELECT
            id_venta, 
            'DAM' AS Origen,
            nombre_desarrollo AS Desarrollo,
            privada AS Privada,  
            numero_etapa AS Etapa,
            numero_unidad AS Unidad, 
            NombreAsesor, ApellidoPaternoAsesor, ApellidoMaternoAsesor,
            NombreCliente, ApellidoPCliente, ApellidoMCliente, 
            CopropietariosConcatenados AS NombreCoproConcatenado,
            telefono_celular,
            correo_electronico, 
            cuentaBeneficiario, 
            nombre_status, 
            referencia_banco,
            Fecha_Contrato,
            Aprobado_juridico AS Fecha_Firma_Contrato,
            precio_venta,  
            dia_pago,
            total_pagado AS Total_cobrado,
            cantidad_enganche,
            saldo_total AS total_por_cobrar,
            (numero_pagos) + 1 AS Meses_Financia,
            Mensualidad,
            DATE_DIFF(CURRENT_DATE(), primera_fecha_vencido, DAY) AS dias_en_mora,
            saldo_vencido_total,
            total_requerido,
            pago_vencido_total,
            numero_pago,
            DATE(fecha_ultimo_ingreso) AS fecha_ultimo_ingreso,
            monto_ultimo_ingreso,
            monto_ultimo_ingreso_cobrado,
            siguiente_fecha_pago,
            DATE(fecha_promesa) AS fecha_promesa
        FROM EXTERNAL_QUERY("terraviva-439415.us.dam",
          """SELECT 
                  v.id_venta, 
                    NULLIF(v.fecha_cierre_venta, '0000-00-00') AS Fecha_Contrato,
                    v.precio_venta,   
                    v.total_pagado,
                    v.cantidad_enganche, 
                    v.saldo_total,
                    v.numero_pagos,
                    u.nombre AS NombreAsesor, 
                    u.apellido_paterno AS ApellidoPaternoAsesor, 
                    u.apellido_materno AS ApellidoMaternoAsesor,
                    c.nombre AS NombreCliente, 
                    c.apellido_p AS ApellidoPCliente, 
                    c.apellido_m AS ApellidoMCliente,
                    cop.CopropietariosConcatenados,

                    c.telefono_celular,
                    c.correo_electronico,
                    d.nombre_desarrollo, 
                    uni.privada, 
                    uni.numero_etapa, 
                    uni.numero_unidad, 
                    uni.modelo, 
                    uni.referencia_banco,  
                    sv.nombre AS nombre_status, 
                    DATE(i.fecha_ultimo_ingreso) AS fecha_ultimo_ingreso,
                    i.monto_ultimo_ingreso,
                    monto_ultimo_ingreso_cobrado,
                    stp.cuentaBeneficiario,
                    tp.dia_pago,
                    tp.Mensualidad,
                  bv.Aprobado_juridico,  
                  venc.primera_fecha_vencido,
                  venc.saldo_vencido_total,
                  venc.total_requerido,
                  venc.pago_vencido_total,
                  venc.numero_pago,
                  DATE(venc.siguiente_fecha_pago) AS siguiente_fecha_pago,
                  DATE(fp.fecha_promesa) AS fecha_promesa
                             
           FROM venta AS v
           LEFT JOIN (
                        SELECT 
                           i.id_venta,
                           DATE(NULLIF(i.fecha_ingreso, '0000-00-00')) AS fecha_ultimo_ingreso,
                           i.monto_ingresado AS monto_ultimo_ingreso
                        FROM ingreso i
                        INNER JOIN (
                           SELECT 
                               id_venta,
                               MAX(id_ingreso) as max_id
                           FROM ingreso
                           WHERE fecha_aprobacion IS NOT NULL
                           GROUP BY id_venta
                        ) ultimo ON i.id_ingreso = ultimo.max_id
                        WHERE i.fecha_aprobacion IS NOT NULL
                        
                        ) AS i ON v.id_venta = i.id_venta 
           LEFT JOIN (
                         SELECT 
                             i.id_venta,
                             DATE(NULLIF(i.fecha_ingreso, '0000-00-00')) AS fecha_ultimo_ingreso,
                             CASE 
                                 WHEN EXTRACT(YEAR FROM DATE(NULLIF(i.fecha_ingreso, '0000-00-00'))) = EXTRACT(YEAR FROM CURRENT_DATE())
                                 AND EXTRACT(MONTH FROM DATE(NULLIF(i.fecha_ingreso, '0000-00-00'))) = EXTRACT(MONTH FROM CURRENT_DATE())
                                 THEN i.monto_ingresado
                                 ELSE 0
                             END AS monto_ultimo_ingreso_cobrado
                         FROM ingreso i
                         INNER JOIN (
                             SELECT 
                                 id_venta,
                                 MAX(id_ingreso) as max_id
                             FROM ingreso
                             WHERE fecha_aprobacion IS NOT NULL
                             GROUP BY id_venta
                         ) ultimo ON i.id_ingreso = ultimo.max_id
                         WHERE i.fecha_aprobacion IS NOT NULL
                     ) AS ia ON v.id_venta = ia.id_venta
           LEFT JOIN (SELECT id_venta, MAX(DATE(NULLIF(fecha_promesa, '0000-00-00'))) AS fecha_promesa FROM seguimiento GROUP BY id_venta) AS fp ON v.id_venta = fp.id_venta
           LEFT JOIN (SELECT id_venta, cuentaBeneficiario FROM stp_bitacora WHERE status = 1 GROUP BY id_venta) AS stp ON v.id_venta = stp.id_venta
           LEFT JOIN usuario AS u ON v.id_usuario = u.id_usuario 
           LEFT JOIN (
            SELECT 
                id_venta,
                GROUP_CONCAT(
                    CASE 
                        -- Si todos los campos están vacíos, omitir
                        WHEN TRIM(CONCAT(
                            COALESCE(nombre, ''),
                            COALESCE(apellido_p, ''),
                            COALESCE(apellido_m, '')
                        )) = '' THEN NULL
                        -- Construir nombre completo sin espacios extra
                        ELSE TRIM(
                            CONCAT_WS(' ',
                                NULLIF(TRIM(nombre), ''),
                                NULLIF(TRIM(apellido_p), ''),
                                NULLIF(TRIM(apellido_m), '')
                            )
                        )
                    END SEPARATOR ', '
                ) AS CopropietariosConcatenados
            FROM copropietario 
            GROUP BY id_venta
            HAVING GROUP_CONCAT(
                TRIM(CONCAT(
                    COALESCE(nombre, ''),
                    COALESCE(apellido_p, ''),
                    COALESCE(apellido_m, '')
                ))
            ) IS NOT NULL
            ) AS cop ON v.id_cliente = cop.id_venta
           LEFT JOIN cliente AS c ON v.id_cliente = c.id_cliente
           LEFT JOIN unidades AS uni ON v.id_unidad = uni.id_unidad LEFT JOIN desarrollo AS d ON uni.id_desarrollo = d.id_desarrollo
           LEFT JOIN status_venta AS sv ON v.status_venta = sv.id_status
           LEFT JOIN (
                        SELECT 
                            tap.id_venta, 
                            MAX(EXTRACT(DAY FROM  fecha_pago )) AS dia_pago, 
                            pago_total AS Mensualidad
                        FROM tabla_pagos AS tap
                        LEFT JOIN tipo_pago AS tip ON tap.id_tipo_pago = tip.id_tipo_pago
                        WHERE tip.nombre_tipo_pago = 'Mensualidad'
                        GROUP BY tap.id_venta
                      ) AS tp ON v.id_venta = tp.id_venta
           LEFT JOIN (
                        SELECT 
                            tap.id_venta,
                                 MIN(CASE  WHEN tap.saldo_pago_total != 0  AND DATE(tap.fecha_pago) < CURRENT_DATE()  THEN DATE(tap.fecha_pago)  END) AS primera_fecha_vencido,
                                 SUM(CASE WHEN tap.saldo_pago_total != 0  AND DATE(tap.fecha_pago) < CURRENT_DATE() THEN tap.saldo_pago_total ELSE 0 END) AS saldo_vencido_total,
                                 SUM(CASE WHEN tap.saldo_pago_total != 0 AND DATE(tap.fecha_pago) < CURRENT_DATE() THEN tap.saldo_pago_total ELSE 0 END) +
                                 COALESCE(
                                     (SELECT tap2.saldo_pago_total 
                                      FROM tabla_pagos tap2 
                                      WHERE tap2.id_venta = tap.id_venta 
                                        AND tap2.saldo_pago_total != 0 
                                        AND DATE(tap2.fecha_pago) >= CURRENT_DATE()
                                      ORDER BY tap2.fecha_pago ASC 
                                      LIMIT 1),
                                     0
                                 ) AS total_requerido,
                                 SUM(CASE WHEN tap.saldo_pago_total != 0  AND DATE(tap.fecha_pago) < CURRENT_DATE() THEN tap.pago_total ELSE 0 END) AS pago_vencido_total,
                                 MIN(CASE WHEN tap.saldo_pago_total != 0 AND DATE(tap.fecha_pago) >= CURRENT_DATE() THEN DATE(tap.fecha_pago) END) AS siguiente_fecha_pago,
                                 MIN(CASE WHEN tap.saldo_pago_total != 0 AND DATE(tap.fecha_pago) >= CURRENT_DATE() THEN numero_pago END) AS numero_pago
                        FROM tabla_pagos AS tap
                        LEFT JOIN tipo_pago AS tip 
                               ON tap.id_tipo_pago = tip.id_tipo_pago 
                        GROUP BY tap.id_venta
                    ) AS venc ON v.id_venta = venc.id_venta
           LEFT JOIN (
                       SELECT
                           bv.id_venta,
                           MIN(CASE WHEN sv2.nombre LIKE 'Aprobado jur%' THEN DATE(bv.fecha_movimiento) END) AS Aprobado_juridico
                       FROM `bitacora_venta` AS bv
                       LEFT JOIN status_venta sv2 ON bv.id_status_venta = sv2.id_status
                       GROUP BY bv.id_venta
                     ) AS bv ON v.id_venta = bv.id_venta

           WHERE v.status_venta IN (7, 8)""")  
            GROUP BY id_venta, Origen, nombre_desarrollo, privada, numero_etapa, numero_unidad,  NombreAsesor, ApellidoPaternoAsesor, ApellidoMaternoAsesor, NombreCliente, ApellidoPCliente, ApellidoMCliente,  CopropietariosConcatenados, telefono_celular, correo_electronico,  cuentaBeneficiario,  nombre_status,  referencia_banco, Fecha_Contrato, Aprobado_juridico, precio_venta, dia_pago, total_pagado, cantidad_enganche, saldo_total, numero_pagos, Mensualidad, primera_fecha_vencido, saldo_vencido_total, total_requerido,  pago_vencido_total, numero_pago, fecha_ultimo_ingreso, monto_ultimo_ingreso, monto_ultimo_ingreso_cobrado,  siguiente_fecha_pago, fecha_promesa

    UNION ALL

    -- Terraviva
        SELECT
            id_venta, 
            'Terraviva' AS Origen,
            nombre_desarrollo AS Desarrollo,
            privada AS Privada,
            numero_etapa AS Etapa,
            numero_unidad AS Unidad, 
            NombreAsesor, ApellidoPaternoAsesor, ApellidoMaternoAsesor,
            NombreCliente, ApellidoPCliente, ApellidoMCliente, 
            CopropietariosConcatenados AS NombreCoproConcatenado,
            telefono_celular,
            correo_electronico, 
            cuentaBeneficiario, 
            nombre_status, 
            referencia_banco,
            Fecha_Contrato,
            Aprobado_juridico AS Fecha_Firma_Contrato,
            precio_venta,  
            dia_pago,
            total_pagado AS Total_cobrado,
            cantidad_enganche,
            saldo_total AS total_por_cobrar,
            (numero_pagos) + 1 AS Meses_Financia,
            Mensualidad,
            DATE_DIFF(CURRENT_DATE(), primera_fecha_vencido, DAY) AS dias_en_mora,
            saldo_vencido_total,
            total_requerido,
            pago_vencido_total,
            numero_pago,
            DATE(fecha_ultimo_ingreso) AS fecha_ultimo_ingreso,
            monto_ultimo_ingreso,
            monto_ultimo_ingreso_cobrado,
            siguiente_fecha_pago,
            DATE(fecha_promesa) AS fecha_promesa
        FROM EXTERNAL_QUERY("terraviva-439415.us.terraviva",
          """SELECT 
                  v.id_venta, 
                    NULLIF(v.fecha_cierre_venta, '0000-00-00') AS Fecha_Contrato,
                    v.precio_venta,   
                    v.total_pagado,
                    v.cantidad_enganche, 
                    v.saldo_total,
                    v.numero_pagos,
                    u.nombre AS NombreAsesor, 
                    u.apellido_paterno AS ApellidoPaternoAsesor, 
                    u.apellido_materno AS ApellidoMaternoAsesor,
                    c.nombre AS NombreCliente, 
                    c.apellido_p AS ApellidoPCliente, 
                    c.apellido_m AS ApellidoMCliente,

                    cop.CopropietariosConcatenados,
                    c.telefono_celular,
                    c.correo_electronico,
                    d.nombre_desarrollo, 
                    uni.privada, 
                    uni.numero_etapa, 
                    uni.numero_unidad, 
                    uni.modelo,   
                    uni.referencia_banco,
                    sv.nombre AS nombre_status, 
                    DATE(i.fecha_ultimo_ingreso) AS fecha_ultimo_ingreso,
                    i.monto_ultimo_ingreso,
                    monto_ultimo_ingreso_cobrado,
                    stp.cuentaBeneficiario,
                    tp.dia_pago,
                    tp.Mensualidad,
                  bv.Aprobado_juridico,  
                  venc.primera_fecha_vencido,
                  venc.saldo_vencido_total,
                  venc.total_requerido,
                  venc.pago_vencido_total,
                  venc.numero_pago,
                  DATE(venc.siguiente_fecha_pago) AS siguiente_fecha_pago,
                  DATE(fp.fecha_promesa) AS fecha_promesa
                             
           FROM venta AS v
           LEFT JOIN (
                        SELECT 
                           i.id_venta,
                           DATE(NULLIF(i.fecha_ingreso, '0000-00-00')) AS fecha_ultimo_ingreso,
                           i.monto_ingresado AS monto_ultimo_ingreso
                        FROM ingreso i
                        INNER JOIN (
                           SELECT 
                               id_venta,
                               MAX(id_ingreso) as max_id
                           FROM ingreso
                           WHERE fecha_aprobacion IS NOT NULL
                           GROUP BY id_venta
                        ) ultimo ON i.id_ingreso = ultimo.max_id
                        WHERE i.fecha_aprobacion IS NOT NULL
                        
                        ) AS i ON v.id_venta = i.id_venta 
           LEFT JOIN (
                         SELECT 
                             i.id_venta,
                             DATE(NULLIF(i.fecha_ingreso, '0000-00-00')) AS fecha_ultimo_ingreso,
                             CASE 
                                 WHEN EXTRACT(YEAR FROM DATE(NULLIF(i.fecha_ingreso, '0000-00-00'))) = EXTRACT(YEAR FROM CURRENT_DATE())
                                 AND EXTRACT(MONTH FROM DATE(NULLIF(i.fecha_ingreso, '0000-00-00'))) = EXTRACT(MONTH FROM CURRENT_DATE())
                                 THEN i.monto_ingresado
                                 ELSE 0
                             END AS monto_ultimo_ingreso_cobrado
                         FROM ingreso i
                         INNER JOIN (
                             SELECT 
                                 id_venta,
                                 MAX(id_ingreso) as max_id
                             FROM ingreso
                             WHERE fecha_aprobacion IS NOT NULL
                             GROUP BY id_venta
                         ) ultimo ON i.id_ingreso = ultimo.max_id
                         WHERE i.fecha_aprobacion IS NOT NULL
                     ) AS ia ON v.id_venta = ia.id_venta
           LEFT JOIN (SELECT id_venta, MAX(DATE(NULLIF(fecha_promesa, '0000-00-00'))) AS fecha_promesa FROM seguimiento GROUP BY id_venta) AS fp ON v.id_venta = fp.id_venta
           LEFT JOIN (SELECT id_venta, cuentaBeneficiario FROM stp_bitacora WHERE status = 1 GROUP BY id_venta) AS stp ON v.id_venta = stp.id_venta
           LEFT JOIN usuario AS u ON v.id_usuario = u.id_usuario 
           LEFT JOIN cliente AS c ON v.id_cliente = c.id_cliente
           LEFT JOIN (
            SELECT 
                id_venta,
                GROUP_CONCAT(
                    CASE 
                        -- Si todos los campos están vacíos, omitir
                        WHEN TRIM(CONCAT(
                            COALESCE(nombre, ''),
                            COALESCE(apellido_p, ''),
                            COALESCE(apellido_m, '')
                        )) = '' THEN NULL
                        -- Construir nombre completo sin espacios extra
                        ELSE TRIM(
                            CONCAT_WS(' ',
                                NULLIF(TRIM(nombre), ''),
                                NULLIF(TRIM(apellido_p), ''),
                                NULLIF(TRIM(apellido_m), '')
                            )
                        )
                    END SEPARATOR ', '
                ) AS CopropietariosConcatenados
            FROM copropietario 
            GROUP BY id_venta
            HAVING GROUP_CONCAT(
                TRIM(CONCAT(
                    COALESCE(nombre, ''),
                    COALESCE(apellido_p, ''),
                    COALESCE(apellido_m, '')
                ))
            ) IS NOT NULL
            ) AS cop ON v.id_cliente = cop.id_venta
           LEFT JOIN unidades AS uni ON v.id_unidad = uni.id_unidad LEFT JOIN desarrollo AS d ON uni.id_desarrollo = d.id_desarrollo
           LEFT JOIN status_venta AS sv ON v.status_venta = sv.id_status
           LEFT JOIN (
                        SELECT 
                            tap.id_venta, 
                            MAX(EXTRACT(DAY FROM  fecha_pago )) AS dia_pago, 
                            pago_total AS Mensualidad
                        FROM tabla_pagos AS tap
                        LEFT JOIN tipo_pago AS tip ON tap.id_tipo_pago = tip.id_tipo_pago
                        WHERE tip.nombre_tipo_pago = 'Mensualidad'
                        GROUP BY tap.id_venta
                      ) AS tp ON v.id_venta = tp.id_venta
           LEFT JOIN (
                        SELECT 
                            tap.id_venta,
                                 MIN(CASE  WHEN tap.saldo_pago_total != 0  AND DATE(tap.fecha_pago) < CURRENT_DATE()  THEN DATE(tap.fecha_pago)  END) AS primera_fecha_vencido,
                                 SUM(CASE WHEN tap.saldo_pago_total != 0  AND DATE(tap.fecha_pago) < CURRENT_DATE() THEN tap.saldo_pago_total ELSE 0 END) AS saldo_vencido_total,
                                 SUM(CASE WHEN tap.saldo_pago_total != 0 AND DATE(tap.fecha_pago) < CURRENT_DATE() THEN tap.saldo_pago_total ELSE 0 END) +
                                 COALESCE(
                                     (SELECT tap2.saldo_pago_total 
                                      FROM tabla_pagos tap2 
                                      WHERE tap2.id_venta = tap.id_venta 
                                        AND tap2.saldo_pago_total != 0 
                                        AND DATE(tap2.fecha_pago) >= CURRENT_DATE()
                                      ORDER BY tap2.fecha_pago ASC 
                                      LIMIT 1),
                                     0
                                 ) AS total_requerido,
                                 SUM(CASE WHEN tap.saldo_pago_total != 0  AND DATE(tap.fecha_pago) < CURRENT_DATE() THEN tap.pago_total ELSE 0 END) AS pago_vencido_total,
                                 MIN(CASE WHEN tap.saldo_pago_total != 0 AND DATE(tap.fecha_pago) >= CURRENT_DATE() THEN DATE(tap.fecha_pago) END) AS siguiente_fecha_pago,
                                 MIN(CASE WHEN tap.saldo_pago_total != 0 AND DATE(tap.fecha_pago) >= CURRENT_DATE() THEN numero_pago END) AS numero_pago
                        FROM tabla_pagos AS tap
                        LEFT JOIN tipo_pago AS tip 
                               ON tap.id_tipo_pago = tip.id_tipo_pago 
                        GROUP BY tap.id_venta
                    ) AS venc ON v.id_venta = venc.id_venta
           LEFT JOIN (
                       SELECT
                           bv.id_venta,
                           MIN(CASE WHEN sv2.nombre LIKE 'Aprobado jur%' THEN DATE(bv.fecha_movimiento) END) AS Aprobado_juridico
                       FROM `bitacora_venta` AS bv
                       LEFT JOIN status_venta sv2 ON bv.id_status_venta = sv2.id_status
                       GROUP BY bv.id_venta
                     ) AS bv ON v.id_venta = bv.id_venta

           WHERE v.status_venta IN (7, 8)""")  
            GROUP BY id_venta, Origen, nombre_desarrollo, privada, numero_etapa, numero_unidad,  NombreAsesor, ApellidoPaternoAsesor, ApellidoMaternoAsesor, NombreCliente, ApellidoPCliente, ApellidoMCliente,  CopropietariosConcatenados, telefono_celular, correo_electronico,  cuentaBeneficiario,  nombre_status,  referencia_banco, Fecha_Contrato, Aprobado_juridico, precio_venta, dia_pago, total_pagado, cantidad_enganche, saldo_total, numero_pagos, Mensualidad, primera_fecha_vencido, saldo_vencido_total, total_requerido, pago_vencido_total, numero_pago, fecha_ultimo_ingreso, monto_ultimo_ingreso, monto_ultimo_ingreso_cobrado, siguiente_fecha_pago, fecha_promesa

    UNION ALL

    --Almaviva
        SELECT
            --SELECT id_venta, MAX(DATE(NULLIF(fecha_ingreso,    '0000-00-00'))) AS fecha_ultimo_ingreso, monto_ingreso  FROM ingreso    GROUP BY id_venta) AS i ON v.id_venta = i.id_venta
            id_venta, 
            'Almaviva' AS Origen,
            nombre_desarrollo AS Desarrollo,
            privada AS Privada,
            numero_etapa AS Etapa,
            numero_unidad AS Unidad, 
            NombreAsesor, ApellidoPaternoAsesor, ApellidoMaternoAsesor,
            NombreCliente, ApellidoPCliente, ApellidoMCliente, 
            CopropietariosConcatenados AS NombreCoproConcatenado,
            telefono_celular,
            correo_electronico, 
            cuentaBeneficiario, 
            nombre_status, 
            referencia_banco,
            Fecha_Contrato,
            Aprobado_juridico AS Fecha_Firma_Contrato,
            precio_venta,  
            dia_pago,
            total_pagado AS Total_cobrado,
            cantidad_enganche,
            saldo_total AS total_por_cobrar,
            (numero_pagos) + 1 AS Meses_Financia,
            Mensualidad,
            DATE_DIFF(CURRENT_DATE(), primera_fecha_vencido, DAY) AS dias_en_mora,
            saldo_vencido_total,
            total_requerido,
            pago_vencido_total,
            numero_pago,
            DATE(fecha_ultimo_ingreso) AS fecha_ultimo_ingreso,
            monto_ultimo_ingreso,
            monto_ultimo_ingreso_cobrado,
            siguiente_fecha_pago,
            DATE(fecha_promesa) AS fecha_promesa
        FROM EXTERNAL_QUERY("terraviva-439415.us.bq_almaviva",
          """SELECT 
                  v.id_venta, 
                    NULLIF(v.fecha_cierre_venta, '0000-00-00') AS Fecha_Contrato,
                    v.precio_venta,   
                    v.total_pagado,
                    v.cantidad_enganche, 
                    v.saldo_total,
                    v.numero_pagos,
                    u.nombre AS NombreAsesor, 
                    u.apellido_paterno AS ApellidoPaternoAsesor, 
                    u.apellido_materno AS ApellidoMaternoAsesor,
                    c.nombre AS NombreCliente, 
                    c.apellido_p AS ApellidoPCliente, 
                    c.apellido_m AS ApellidoMCliente,
                    cop.CopropietariosConcatenados,
                    c.telefono_celular,
                    c.correo_electronico,
                    d.nombre_desarrollo, 
                    uni.privada, 
                    uni.numero_etapa, 
                    uni.numero_unidad, 
                    uni.modelo,   
                    uni.referencia_banco,
                    sv.nombre AS nombre_status, 
                    DATE(i.fecha_ultimo_ingreso) AS fecha_ultimo_ingreso,
                    monto_ultimo_ingreso,
                    monto_ultimo_ingreso_cobrado,
                    stp.cuentaBeneficiario,
                    tp.dia_pago,
                    tp.Mensualidad,
                  bv.Aprobado_juridico,  
                  venc.primera_fecha_vencido,
                  venc.saldo_vencido_total,
                  venc.total_requerido,
                  venc.pago_vencido_total,
                  venc.numero_pago,
                  DATE(venc.siguiente_fecha_pago) AS siguiente_fecha_pago,
                  DATE(fp.fecha_promesa) AS fecha_promesa
                             

           FROM venta AS v
           LEFT JOIN (
                        SELECT 
                           i.id_venta,
                           DATE(NULLIF(i.fecha_ingreso, '0000-00-00')) AS fecha_ultimo_ingreso,
                           i.monto_ingresado AS monto_ultimo_ingreso
                        FROM ingreso i
                        INNER JOIN (
                           SELECT 
                               id_venta,
                               MAX(id_ingreso) as max_id
                           FROM ingreso
                           WHERE fecha_aprobacion IS NOT NULL
                           GROUP BY id_venta
                        ) ultimo ON i.id_ingreso = ultimo.max_id
                        WHERE i.fecha_aprobacion IS NOT NULL
                        
                        ) AS i ON v.id_venta = i.id_venta 
           LEFT JOIN (
                         SELECT 
                             i.id_venta,
                             DATE(NULLIF(i.fecha_ingreso, '0000-00-00')) AS fecha_ultimo_ingreso,
                             CASE 
                                 WHEN EXTRACT(YEAR FROM DATE(NULLIF(i.fecha_ingreso, '0000-00-00'))) = EXTRACT(YEAR FROM CURRENT_DATE())
                                 AND EXTRACT(MONTH FROM DATE(NULLIF(i.fecha_ingreso, '0000-00-00'))) = EXTRACT(MONTH FROM CURRENT_DATE())
                                 THEN i.monto_ingresado
                                 ELSE 0
                             END AS monto_ultimo_ingreso_cobrado
                         FROM ingreso i
                         INNER JOIN (
                             SELECT 
                                 id_venta,
                                 MAX(id_ingreso) as max_id
                             FROM ingreso
                             WHERE fecha_aprobacion IS NOT NULL
                             GROUP BY id_venta
                         ) ultimo ON i.id_ingreso = ultimo.max_id
                         WHERE i.fecha_aprobacion IS NOT NULL
                     ) AS ia ON v.id_venta = ia.id_venta
           LEFT JOIN (SELECT id_venta, MAX(DATE(NULLIF(fecha_promesa, '0000-00-00'))) AS fecha_promesa FROM seguimiento GROUP BY id_venta) AS fp ON v.id_venta = fp.id_venta
           LEFT JOIN (SELECT id_venta, cuentaBeneficiario FROM stp_bitacora WHERE status = 1 GROUP BY id_venta) AS stp ON v.id_venta = stp.id_venta
           LEFT JOIN usuario AS u ON v.id_usuario = u.id_usuario 
           LEFT JOIN cliente AS c ON v.id_cliente = c.id_cliente
           LEFT JOIN (
            SELECT 
                id_venta,
                GROUP_CONCAT(
                    CASE 
                        -- Si todos los campos están vacíos, omitir
                        WHEN TRIM(CONCAT(
                            COALESCE(nombre, ''),
                            COALESCE(apellido_p, ''),
                            COALESCE(apellido_m, '')
                        )) = '' THEN NULL
                        -- Construir nombre completo sin espacios extra
                        ELSE TRIM(
                            CONCAT_WS(' ',
                                NULLIF(TRIM(nombre), ''),
                                NULLIF(TRIM(apellido_p), ''),
                                NULLIF(TRIM(apellido_m), '')
                            )
                        )
                    END SEPARATOR ', '
                ) AS CopropietariosConcatenados
            FROM copropietario 
            GROUP BY id_venta
            HAVING GROUP_CONCAT(
                TRIM(CONCAT(
                    COALESCE(nombre, ''),
                    COALESCE(apellido_p, ''),
                    COALESCE(apellido_m, '')
                ))
            ) IS NOT NULL
            ) AS cop ON v.id_cliente = cop.id_venta
           LEFT JOIN unidades AS uni ON v.id_unidad = uni.id_unidad LEFT JOIN desarrollo AS d ON uni.id_desarrollo = d.id_desarrollo
           LEFT JOIN status_venta AS sv ON v.status_venta = sv.id_status
           LEFT JOIN (
                        SELECT 
                            tap.id_venta, 
                            MAX(EXTRACT(DAY FROM  fecha_pago )) AS dia_pago, 
                            pago_total AS Mensualidad
                        FROM tabla_pagos AS tap
                        LEFT JOIN tipo_pago AS tip ON tap.id_tipo_pago = tip.id_tipo_pago
                        WHERE tip.nombre_tipo_pago = 'Mensualidad'
                        GROUP BY tap.id_venta
                      ) AS tp ON v.id_venta = tp.id_venta
           LEFT JOIN (
                        SELECT 
                            tap.id_venta,
                                 MIN(CASE WHEN tap.saldo_pago_total != 0  AND DATE(tap.fecha_pago) <  CURRENT_DATE()  THEN DATE(tap.fecha_pago)         END) AS primera_fecha_vencido,
                                 SUM(CASE WHEN tap.saldo_pago_total != 0  AND DATE(tap.fecha_pago) <  CURRENT_DATE()  THEN tap.saldo_pago_total  ELSE 0 END) AS saldo_vencido_total,

                                 SUM(CASE WHEN tap.saldo_pago_total != 0 AND DATE(tap.fecha_pago) < CURRENT_DATE() THEN tap.saldo_pago_total ELSE 0 END) +
                                 COALESCE(
                                     (SELECT tap2.saldo_pago_total 
                                      FROM tabla_pagos tap2 
                                      WHERE tap2.id_venta = tap.id_venta 
                                        AND tap2.saldo_pago_total != 0 
                                        AND DATE(tap2.fecha_pago) >= CURRENT_DATE()
                                      ORDER BY tap2.fecha_pago ASC 
                                      LIMIT 1),
                                     0
                                 ) AS total_requerido,

                                 SUM(CASE WHEN tap.saldo_pago_total != 0  AND DATE(tap.fecha_pago) <  CURRENT_DATE()  THEN tap.saldo_pago_total  ELSE 0 END) AS total_requerido_prueba,
                                 SUM(CASE WHEN tap.saldo_pago_total != 0  AND DATE(tap.fecha_pago) <  CURRENT_DATE()  THEN tap.pago_total        ELSE 0 END) AS pago_vencido_total,
                                 MIN(CASE WHEN tap.saldo_pago_total != 0  AND DATE(tap.fecha_pago) >= CURRENT_DATE()  THEN DATE(tap.fecha_pago)         END) AS siguiente_fecha_pago,
                                 MIN(CASE WHEN tap.saldo_pago_total != 0  AND DATE(tap.fecha_pago) >= CURRENT_DATE()  THEN numero_pago                  END) AS numero_pago
                        FROM tabla_pagos AS tap
                        LEFT JOIN tipo_pago AS tip 
                               ON tap.id_tipo_pago = tip.id_tipo_pago
                        GROUP BY tap.id_venta
                    ) AS venc ON v.id_venta = venc.id_venta
           LEFT JOIN (
                       SELECT
                           bv.id_venta,
                           MIN(CASE WHEN sv2.nombre LIKE 'Aprobado jur%' THEN DATE(bv.fecha_movimiento) END) AS Aprobado_juridico
                       FROM `bitacora_venta` AS bv
                       LEFT JOIN status_venta sv2 ON bv.id_status_venta = sv2.id_status
                       GROUP BY bv.id_venta
                     ) AS bv ON v.id_venta = bv.id_venta

           WHERE v.status_venta IN (7, 8)""")
            GROUP BY id_venta, Origen, nombre_desarrollo, privada, numero_etapa, numero_unidad,  NombreAsesor, ApellidoPaternoAsesor, ApellidoMaternoAsesor, NombreCliente, ApellidoPCliente, ApellidoMCliente,  CopropietariosConcatenados, telefono_celular, correo_electronico,  cuentaBeneficiario,  nombre_status,  referencia_banco, Fecha_Contrato, Aprobado_juridico, precio_venta, dia_pago, total_pagado, cantidad_enganche, saldo_total, numero_pagos, Mensualidad, primera_fecha_vencido, saldo_vencido_total, total_requerido, pago_vencido_total, numero_pago, fecha_ultimo_ingreso, monto_ultimo_ingreso, monto_ultimo_ingreso_cobrado, siguiente_fecha_pago, fecha_promesa
    ),
    VentasFinal AS ( --LIMPIEZA DE DATOS
        SELECT
            v.id_venta,
            CASE
                WHEN va.Desarrollo = "Puerto Telchac" THEN  CONCAT(v.Desarrollo, ' ', v.Unidad, ' ', v.Etapa)
                ELSE CONCAT(v.Desarrollo, ' ', v.Unidad) 
            END AS id,
            va.Marca,
            va.Desarrollo,
            va.privada AS Privada,
            v.Etapa,
            v.Unidad,
            --Cliente
            TRIM(REGEXP_REPLACE(
                CONCAT(
                    INITCAP(TRIM(v.NombreAsesor)), ' ', 
                    INITCAP(TRIM(REPLACE(v.ApellidoPaternoAsesor,'-',''))), ' ', 
                    INITCAP(TRIM(REPLACE(v.ApellidoMaternoAsesor,'-','')))
                ),
                r'Merida|\.|Miami|Cdmx|Dam|Interno|Externo', ''
            )) AS Asesor,
            TRIM( 
                CONCAT(
                    INITCAP(TRIM(v.NombreCliente)), ' ', 
                    INITCAP(TRIM(v.ApellidoPCliente)), ' ', 
                    INITCAP(TRIM(v.ApellidoMCliente))
            )) AS Cliente,
            INITCAP(TRIM(v.NombreCoproConcatenado)) AS Copropietario, 
            v.telefono_celular,
            v.correo_electronico,   
            v.referencia_banco AS CuanetaBeneficiarioReal,
            v.cuentaBeneficiario, 
            v.nombre_status AS Estatus,
            v.Fecha_Contrato,
            v.Fecha_Firma_Contrato,
            v.precio_venta,  
            v.dia_pago,
            v.Total_cobrado,
            CASE 
                WHEN v.cantidad_enganche > v.Total_cobrado THEN v.Total_cobrado
                ELSE v.cantidad_enganche
            END  Enganche_pagado,
            v.total_por_cobrar,
            v.Meses_Financia,
            v.Mensualidad,
            v.Total_Requerido ,
            v.dias_en_mora AS Dias_Atrasado, 
            v.fecha_ultimo_ingreso,
            v.saldo_vencido_total AS Acumado_Vencido,
            v.numero_pago,
            v.siguiente_fecha_pago,
            v.monto_ultimo_ingreso,
            v.monto_ultimo_ingreso_cobrado,
            CASE 
                WHEN v.dias_en_mora IS NULL THEN NULL
                ELSE  v.fecha_promesa
            END fecha_promesa

        FROM VentasConsolidades AS v
        LEFT JOIN `Dimensiones.NombreDesarrollo` AS va ON va.id_nombre_desarrollo = v.Desarrollo
    )

    SELECT 
            id_venta,
            id,
            Marca,
            Desarrollo,
            Privada,
            Etapa,
            Unidad,
            REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(Cliente, 'á', 'a'), 'é', 'e'), 'í', 'i'), 'ó', 'o'), 'ú', 'u'), 'ñ', 'n') AS Cliente,
            REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(Cliente, 'á', 'a'), 'é', 'e'), 'í', 'i'), 'ó', 'o'), 'ú', 'u'), 'ñ', 'n') AS Copropietario,
            Asesor,
            nv.Sucursal,
            nv.Tipo,
            nv.Equipo,
            telefono_celular,
            correo_electronico,   
            CONCAT('stp_', CuanetaBeneficiarioReal) AS CuanetaBeneficiarioReal, 
            Estatus,
            Fecha_Contrato,
            Fecha_Firma_Contrato,
            precio_venta,  
            dia_pago,
            Total_cobrado,
            Enganche_pagado,
            total_por_cobrar,
            Meses_Financia,
            Mensualidad,
            Total_Requerido, -- ESTA SUMA SOLO SUMA LAS MENSUALIDADES FALTANTES NO CUENTA LOS CARGOS POR ADEUDO(SI QUIERO CONSIDERAR LAS PENALIZAICONES SUMAR 500 PESOS POR CADA PAGO ATRASADO(SALDO))
            Dias_Atrasado,
            fecha_ultimo_ingreso,
            --Cobrado, -- DEPENDE DE LO TOTAL_QUERERIDO ENTONCES SI CAMBIAS TOTAL REQUERIDO , CAMBIA AQUI
            monto_ultimo_ingreso_cobrado AS Cobrado,
            Acumado_Vencido,
            numero_pago,
            siguiente_fecha_pago AS Fecha_Proximo_pago,
            monto_ultimo_ingreso,
            fecha_promesa
    FROM VentasFinal AS v 
    LEFT JOIN `Dimensiones.NombresVendedores` AS nv ON v.Asesor = nv.Vendedor
)
SELECT 
    *
FROM Ventas3Version
GROUP BY id_venta, id, Marca, Desarrollo, Privada, Etapa, Unidad, Cliente, Copropietario, Asesor, Sucursal, Tipo, Equipo, telefono_celular, correo_electronico,  CuanetaBeneficiarioReal,  Estatus, Fecha_Contrato, Fecha_Firma_Contrato, precio_venta,   dia_pago, Total_cobrado, Enganche_pagado, total_por_cobrar, Meses_Financia, Mensualidad, Total_Requerido, Dias_Atrasado, fecha_ultimo_ingreso, Cobrado, Acumado_Vencido, numero_pago, Fecha_Proximo_pago, monto_ultimo_ingreso, fecha_promesa