# -*- coding: utf-8 -*-

def bq_job_stage_f_despacho_productos(table_id,bq_client):
    
    client=bq_client
    
    bq_query = (
                f"""
                
                CREATE OR REPLACE TABLE stage.f_despacho_productos AS
                
                    SELECT
                      idPunto as pkPunto,
                      idProducto as codProducto,
                      cantProducto,
                      nombProducto,
                      IF(LENGTH(creacUsuario)>0,creacUsuario,Null) AS creacUsuario,
                      TIMESTAMP(creacFch) as fchCreacProducto,
                      IF(LENGTH(editaUsuario)>0,editaUsuario,Null) AS editaUsuarioProducto,
                      TIMESTAMP(editaFch) as editaFchProducto,
                      fchCarga
                    FROM
                      landing.despacho_puntos_productos
                    WHERE 
                      LENGTH(idProducto)>0
                    AND 
                        fchCarga= --Get the last modified table
                                    (SELECT DISTINCT fchCarga 
                                    FROM landing.despacho_puntos_productos 
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