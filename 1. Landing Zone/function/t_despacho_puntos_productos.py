from google.cloud import bigquery
from datetime import date, timedelta, datetime, timezone
import pandas as pd


def t_despacho_puntos_productos_to_bq(mysql_conn,table_id,bq_client):
    
    conn=mysql_conn
    client=bq_client
      
    today = date.today()
    d1 = today-timedelta(days = 70)
    d1 = d1.strftime("%Y-%m-%d")
    datetime_load=datetime.utcnow().replace(tzinfo=timezone.utc)
    timestamp_loaded=int(datetime.timestamp(datetime_load)*1000000)

        
    query = f"""SELECT
                idPunto,
                idProducto,
                cantProducto,
                nombProducto,
                creacUsuario,
                creacFch,
                editaUsuario,
                editaFch
                FROM despachopuntosproductos 
                where idPunto IN (SELECT idPunto FROM despachopuntos WHERE fchDespacho >= '{d1}') """
                
    df=pd.read_sql_query(query, conn)

    df["fchCarga"]=datetime_load


    job_config = bigquery.LoadJobConfig(
        schema=[
            bigquery.SchemaField("editaFch", bigquery.enums.SqlTypeNames.DATE),
        ],
        write_disposition="WRITE_APPEND",
    )

    job = client.load_table_from_dataframe(
        df, table_id, job_config=job_config
    )  # Make an API request.
    job.result()  # Wait for the job to complete.

    bq_query = (
            f"SELECT fchCarga, COUNT(1) as contador FROM `{table_id}` "
            f'WHERE fchCarga=TIMESTAMP_MICROS({timestamp_loaded})'
            'GROUP BY fchCarga ORDER BY fchCarga DESC LIMIT 1'
            )
    query_job = client.query(
                            bq_query,
                            location="US",
                            )  # API request - starts the query

    results = query_job.result().to_dataframe()  # Wait for query to complete.
    return(
            "Loaded {} rows in {}.".format(
                results.contador[0],table_id
            )
        )

