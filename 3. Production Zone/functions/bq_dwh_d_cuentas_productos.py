# -*- coding: utf-8 -*-

def bq_job_dwh_d_cuentas_productos(table_id,bq_client):
    
    client=bq_client
    
    bq_query = (
                f"""
                
                --Created at 2021-07-14 by KV
                CREATE OR REPLACE TABLE dwh.d_cuentas_productos AS
                    SELECT * FROM stage.d_cuentas_productos
                    
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