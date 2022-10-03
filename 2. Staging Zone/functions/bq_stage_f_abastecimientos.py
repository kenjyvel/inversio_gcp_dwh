# -*- coding: utf-8 -*-

def bq_job_stage_f_abastecimientos(table_id,bq_client):
    
    client=bq_client
    
    bq_query = (
                f"""
                
CREATE OR REPLACE TABLE stage.f_abastecimiento AS

    SELECT 
    idabastecimiento as pkAbastecimiento,

    EXTRACT(YEAR FROM fecha)*10000
        +EXTRACT(MONTH FROM fecha)*100
        +EXTRACT(DAY FROM fecha) 
        as pkTiempo,

    placa,
    idConductor as pkTrabajadorAbst,

    CASE 
    WHEN (grifo LIKE '%LUISAGAS%' or grifo LIKE'%LUISA GAS%')
        THEN 'LUISAGAS'
    WHEN (grifo LIKE '%ENERGIGAS%')
        THEN 'ENERGIGAS'
    WHEN (grifo LIKE '%SAN DIEGO%')
        THEN 'ESTACION SAN DIEGO'
    ELSE TRIM(grifo) END as nombGrifoAbst,

    UPPER(tipo_combustible) as tipoCombustibleAbst,
    kilometraje_actual as kmActualAbst,
    kilometraje_anterior as kmAnteriorAbst,
    und_medida as undMedidaAbst,
    ROUND(cantidad,2) as cantidadAbst,
    ROUND(importe/cantidad,2) as precioUnitarioAbst,
    ROUND(importe,2) as importeAbst,
    c.estado as estadoAbst,
    modoCreacion as modoCreacionAbst,
    u.usuario as usuarioRegistroAbst,
    fecha_registro as fchRegistroAbst,
    c.usuario as usuarioEdicionAbst, 
    fecha_edicion as fchEdicionAbst,
    c.fchCarga

    FROM landing.combustible c
    LEFT JOIN (SELECT idTrabajador, usuario FROM landing.usuario_incidencia WHERE fchCarga= --Get the last modified table
                    (SELECT DISTINCT fchCarga 
                    FROM landing.usuario_incidencia
                    ORDER BY fchCarga DESC 
                    LIMIT 1)) u ON u.idTrabajador=c.idTrabajador
        WHERE c.fchCarga= --Get the last modified table
                    (SELECT DISTINCT fchCarga 
                    FROM landing.combustible
                    ORDER BY fchCarga DESC 
                    LIMIT 1)
                """
                )
    
    query_job = client.query(
            bq_query,
            location="US",
            )  # API request - starts the query
    
    try:
        (query_job.result())
        return (table_id+' loaded successfully')
    except Exception as e: 
        print(e)
        return (table_id+" can't loaded")