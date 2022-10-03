# -*- coding: utf-8 -*-

def bq_job_stage_f_despacho(table_id,bq_client):
    
    client=bq_client
    
    bq_query = (
                f"""
                
                CREATE OR REPLACE TABLE stage.f_despacho AS
                
                    SELECT
                    stage.pkDespacho(fchDespacho,correlativo) AS pkDespacho,
                    EXTRACT(YEAR FROM fchDespacho)*10000
                        +EXTRACT(MONTH FROM fchDespacho)*100
                        +EXTRACT(DAY FROM fchDespacho) 
                        as pkTiempo,
                    idProgramacion as pkProgramacion,
                    idProducto as pkProducto,
                    placa,
                    idCliente as pkCliente,
                    idProducto in 
                        (SELECT pkProducto FROM stage.d_cuentas_productos where  tipoProd='Normal') 
                        as flgDespacho,
                    stage.tsFechaHora(fchDespacho,hraInicioBase) AS fchDespInicioBase,
                    stage.tsFechaHora(fchDespacho,hraInicio) AS fchDespInicioCliente,
                    stage.tsFechaHora(fchDespachoFinCli,hraFinCliente) AS fchDespFinCliente,
                    stage.tsFechaHora(fchDespachoFin,hraFin) AS fchDespFinBase,
                    kmInicio,
                    kmInicioCliente,
                    kmFinCliente,
                    kmFin,
                    kmFinCliente-kmInicioCliente 
                        as recorridoClienteReal,
                    IF((kmFinCliente-kmInicioCliente)<=450,
                        kmFinCliente-kmInicioCliente,
                        IFNULL(recorridoEsperado,120)
                        ) as recorridoClienteCorregido,
                    IF((kmFinCliente-kmInicioCliente)<=450,
                        'no',
                        'si'
                        ) as recorridoClienteCorregidoFlag,
                    kmFin-kmInicio 
                        as recorridoTotalReal,
                    IF((kmFin-kmInicio)<=450,
                        kmFin-kmInicio,
                        IFNULL(recorridoEsperado,150)
                        ) as recorridoTotalCorregido,
                    IF((kmFin-kmInicio)<=450,
                        'no',
                        'si'
                        ) as recorridoTotalCorregidoFlag,
                    recorridoEsperado as recorridoTotalEsperado,
                    observacion as observDespacho,
                    observCliente,
                    usuario as usuarioCreacDespacho,
                    fchCreacion as fchCreacionDespacho,
                    modoCreacion,
                    modoTerminado,
                    modoConcluido,
                    fchTerminado as fchTerminadoDespacho,
                    usuarioTerminado as usuarioTerminadoDespacho,
                    estadoDespacho,
                    concluido as concluidoDespacho,
                    CASE
                        WHEN concluido='Si' AND estadoDespacho='Terminado' THEN 'Concluido'
                        WHEN concluido='No' AND estadoDespacho='Terminado' THEN 'Terminado'
                        ELSE estadoDespacho
                        END AS estadoFinalDespacho,
                    usuarioGrabaFin,
                    stage.tsFechaHora(fchGrabaFin,hraGrabaFin) as FechaGrabaFin,
                    IF(LENGTH(movilAsignado)>0,movilAsignado,Null) AS movilAsignado,
                    IF(LENGTH(editaUsuarioDesp)>0,editaUsuarioDesp,Null) AS editaUsuarioDesp,
                    IF(LENGTH(usuarioAsignado)>0,usuarioAsignado,Null) AS usuarioAsignado,
                    fchCarga
                    FROM
                    landing.despacho
                    WHERE fchCarga= --Get the last modified table
                                (SELECT DISTINCT fchCarga 
                                FROM landing.despacho 
                                ORDER BY fchCarga DESC 
                                LIMIT 1);
                    
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