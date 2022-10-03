# -*- coding: utf-8 -*-

def bq_job_stage_f_programacion(table_id,bq_client):
    
    client=bq_client
    
    bq_query = (
                f"""
                
                CREATE OR REPLACE TABLE stage.f_programacion AS
                
                    SELECT
                        id as pkProgramacion,
                        EXTRACT(YEAR FROM fchDespacho)*10000
                            +EXTRACT(MONTH FROM fchDespacho)*100
                            +EXTRACT(DAY FROM fchDespacho) 
                            as pkTiempo,
                        idCliente as pkCliente,
                        idProducto as pkProducto,
                        stage.tsFechaHora(fchDespacho,hraInicioEsperada) AS fchInicioEspProg,
                        placa,
                        IF(LENGTH(movilAsignado)>0,movilAsignado,Null) AS movilAsignadoProg,
                        usuAsignado as usuarioAsignadoProg,
                        IF(LENGTH(observacion)>0,observacion,Null) AS obsvProg,
                        estadoProgram as estadoProg,
                        creacUsuario as creacUsuarioProg,
                        creacFch as creacFchProg,
                        IF(LENGTH(editaUsuario)>0,editaUsuario,Null) AS editaUsuarioProg,
                        editaFch as editaFchProg,
                        fchCarga,
                    FROM landing.programacion_despacho
                        WHERE fchCarga= --Get the last modified table
                                    (SELECT DISTINCT fchCarga 
                                    FROM landing.programacion_despacho 
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