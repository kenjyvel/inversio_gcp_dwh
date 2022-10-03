# -*- coding: utf-8 -*-

def bq_job_stage_d_clientes(table_id,bq_client):
    
    client=bq_client
    
    bq_query = (
                f"""
                
                CREATE OR REPLACE TABLE stage.d_clientes AS
                
                  SELECT
                    idRuc as pkCliente,
                    rznSocial as razonSocial,
                    nombre as nombreCliente,
                    estadoCliente,
                    --categCliente,
                    --usuario,
                    TIMESTAMP(fchCreacion) as fchCreacionCliente,
                    IF(LENGTH(usuarioUltimoCambio)>0,usuarioUltimoCambio,Null) AS editaUsuarioCliente,
                    TIMESTAMP(fchUltimoCambio) as editaFchCliente,
                    fchCarga
                  FROM
                    landing.clientes
                  WHERE
                      fchCarga= --Get the last modified table
                                  (SELECT DISTINCT fchCarga 
                                  FROM landing.clientes 
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