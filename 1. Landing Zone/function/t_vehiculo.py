from google.cloud import bigquery
from datetime import date, timedelta, datetime, timezone
import pandas as pd


def t_vehiculo_to_bq(mysql_conn,table_id,bq_client):
    
    conn=mysql_conn
    client=bq_client
      
    datetime_load=datetime.utcnow().replace(tzinfo=timezone.utc)
    timestamp_loaded=int(datetime.timestamp(datetime_load)*1000000)

        
    query = """SELECT
                idPlaca,
                nroVehiculo,
                estado,
                propietario,
                rznSocial,
                considerarPropio,
                nroMotor,
                nroSerie,
                dimLargo,
                dimAlto,
                dimAncho,
                dimInteriorLargo,
                dimInteriorAncho,
                dimInteriorAlto,
                m3Facturable,
                capCombustible,
                rendimiento,
                grupoRendimiento,
                eficCombustible,
                idEficSolesKm,
                combustibleFrec,
                checkAlertas,
                marca,
                modelo,
                anioFabricacion,
                color,
                carroceria,
                puertaCarr,
                clase,
                base,
                cilindros,
                cc,
                hp,
                nroRuedas,
                llantas,
                imgVehiculo,
                pulg,
                pesoSeco,
                pesoUtil,
                pesoBruto,
                pesoUtilReal,
                descripcion,
                kmUltimaMedicion,
                usuario,
                fchCreacion,
                usuarioUltimoCambio,
                fchUltimoCambio,
                fchUltCronJob
                
                FROM vehiculo"""
                
    df=pd.read_sql_query(query, conn)

    df["fchCarga"]=datetime_load


    job_config = bigquery.LoadJobConfig(
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

