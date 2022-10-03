from google.cloud import bigquery
from datetime import date, timedelta, datetime, timezone
import pandas as pd


def t_despacho_to_bq(mysql_conn,table_id,bq_client):
    
    conn=mysql_conn
    client=bq_client
      
    today = date.today()
    d1 = today-timedelta(days = 70)
    d1 = d1.strftime("%Y-%m-%d")
    datetime_load=datetime.utcnow().replace(tzinfo=timezone.utc)
    timestamp_loaded=int(datetime.timestamp(datetime_load)*1000000)

        
    query = f"""SELECT
                    fchDespacho,
                    correlativo,
                    idProgramacion,
                    idProducto,
                    hraInicio,
                    fchDespachoFinCli,
                    fchDespachoFin,
                    hraFin,
                    placa,
                    correlCuenta,
                    idCliente,
                    concluido,
                    hraInicioBase,
                    kmInicio,
                    kmInicioCliente,
                    kmFinCliente,
                    hraFinCliente,
                    kmFin,
                    recorridoEsperado,
                    observacion,
                    observCliente,
                    usuario,
                    fchCreacion,
                    modoCreacion,
                    modoTerminado,
                    modoConcluido,
                    fchTerminado,
                    usuarioTerminado,
                    estadoDespacho,
                    usuarioGrabaFin,
                    fchGrabaFin,
                    hraGrabaFin,
                    movilAsignado,
                    editaUsuarioDesp,
                    usuarioAsignado

                FROM despacho where fchDespacho >= '{d1}' """
                
    df=pd.read_sql_query(query, conn)

    df["hraInicio"]=df["hraInicio"].dt.total_seconds().round().astype(str)
    df["hraFin"]=df["hraFin"].dt.total_seconds().round().astype(str)
    df["hraInicioBase"]=df["hraInicioBase"].dt.total_seconds().round().astype(str)
    df["hraFinCliente"]=df["hraFinCliente"].dt.total_seconds().round().astype(str)
    df["hraGrabaFin"]=df["hraGrabaFin"].dt.total_seconds().round().astype(str)
    df["idCliente"]=df["idCliente"].astype(str)
    df["fchCarga"]=datetime_load

    job_config = bigquery.LoadJobConfig(
        schema=[
            bigquery.SchemaField("fchDespacho", bigquery.enums.SqlTypeNames.DATE),
            bigquery.SchemaField("fchDespachoFinCli", bigquery.enums.SqlTypeNames.DATE),
            bigquery.SchemaField("fchDespachoFin", bigquery.enums.SqlTypeNames.DATE),
            bigquery.SchemaField("placa", bigquery.enums.SqlTypeNames.STRING),
            bigquery.SchemaField("idCliente", bigquery.enums.SqlTypeNames.STRING),
            bigquery.SchemaField("hraInicio", bigquery.enums.SqlTypeNames.STRING),
            bigquery.SchemaField("hraFin", bigquery.enums.SqlTypeNames.STRING),
            bigquery.SchemaField("hraInicioBase", bigquery.enums.SqlTypeNames.STRING),
            bigquery.SchemaField("hraFinCliente", bigquery.enums.SqlTypeNames.STRING),
            bigquery.SchemaField("hraGrabaFin", bigquery.enums.SqlTypeNames.STRING),
            bigquery.SchemaField("fchGrabaFin", bigquery.enums.SqlTypeNames.DATE),
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

