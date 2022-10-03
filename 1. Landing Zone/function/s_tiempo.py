from google.cloud import bigquery
from datetime import date, timedelta, datetime, timezone
import pandas as pd



def s_tiempo_to_bq(table_id,bq_client):
    
    client=bq_client
    
    uri='gs://archives_dwh/dim_tiempo.csv'
      
    job_config = bigquery.LoadJobConfig(
        schema=[
            bigquery.SchemaField("sem_anio", "STRING"),
            bigquery.SchemaField("des_dia", "STRING"),
            bigquery.SchemaField("pk_tiempo", "INT64"),
            bigquery.SchemaField("mes_activo", "INT64"),
            bigquery.SchemaField("evolutivo_activo", "INT64"),
            bigquery.SchemaField("ult_sem_do", "STRING"),
            bigquery.SchemaField("des_fecha", "STRING"),
            bigquery.SchemaField("num_anio", "INT64"),
            bigquery.SchemaField("num_semestre", "INT64"),
            bigquery.SchemaField("num_cuatrimes", "INT64"),
            bigquery.SchemaField("num_trimestre", "INT64"),
            bigquery.SchemaField("num_mes", "INT64"),
            bigquery.SchemaField("aniomes", "STRING"),
            bigquery.SchemaField("dia", "STRING"),
            bigquery.SchemaField("des_mes", "STRING"),
            bigquery.SchemaField("num_sem_anio", "INT64"),
            bigquery.SchemaField("sem_anio_do", "STRING"),
            bigquery.SchemaField("dia_do", "STRING"),
            bigquery.SchemaField("num_dia_anio", "INT64"),
            bigquery.SchemaField("num_dia_mes", "INT64"),
            bigquery.SchemaField("num_dia_semana", "INT64"),
            bigquery.SchemaField("ctd_dias_anio", "INT64"),
            bigquery.SchemaField("ctd_dias_mes", "INT64"),
            bigquery.SchemaField("ctd_dias_semana", "INT64"),
            bigquery.SchemaField("flg_laborable", "INT64"),
            bigquery.SchemaField("flg_fin_semana", "INT64"),
            bigquery.SchemaField("flg_feriado", "INT64"),
            bigquery.SchemaField("flg_festivo", "INT64"),
            bigquery.SchemaField("des_feriado", "STRING"),
            bigquery.SchemaField("des_festivo", "STRING"),
            bigquery.SchemaField("num_anio_cmp", "INT64"),
            bigquery.SchemaField("num_mes_cmp", "INT64"),
            bigquery.SchemaField("num_sem_cmp", "INT64"),
            bigquery.SchemaField("num_anio_cmp_a", "INT64"),
            bigquery.SchemaField("num_mes_cmp_a", "INT64"),
            bigquery.SchemaField("num_sem_cmp_a", "INT64"),
            bigquery.SchemaField("pk_tiempo_ant", "INT64"),
            bigquery.SchemaField("fch_carga", "STRING"),
            bigquery.SchemaField("num_dia_cmp", "INT64"),
            bigquery.SchemaField("num_dia_cmp_a", "INT64"),
            bigquery.SchemaField("pk_tiempo_eq_rrhh", "INT64"),
            bigquery.SchemaField("num_anio_rrhh", "INT64"),
            bigquery.SchemaField("num_semestre_rrhh", "INT64"),
            bigquery.SchemaField("num_cuatrimes_rrhh", "INT64"),
            bigquery.SchemaField("num_trimestre_rrhh", "INT64"),
            bigquery.SchemaField("num_mes_rrhh", "INT64"),
            bigquery.SchemaField("num_sem_anio_iso", "INT64"),
            bigquery.SchemaField("pk_tiempo_ant2", "INT64"),
            bigquery.SchemaField("des_dia_agrupado_1", "STRING"),
            bigquery.SchemaField("des_dia_agrupado_2", "STRING"),
            bigquery.SchemaField("des_dia_agrupado_3", "STRING"),
            bigquery.SchemaField("des_dia_agrupado_4", "STRING"),
            bigquery.SchemaField("des_dia_agrupado_5", "STRING"),
            bigquery.SchemaField("desc_semana_do", "STRING"),
        ],
        write_disposition="WRITE_TRUNCATE",
        skip_leading_rows=1,
        # The source format defaults to CSV, so the line below is optional.
        source_format=bigquery.SourceFormat.CSV,
    )


    job = client.load_table_from_uri(
            uri, table_id, job_config=job_config
        )  # Make an API request.
    job.result()  # Wait for the job to complete.


    query = (
        f"SELECT COUNT(*) as contador FROM `{table_id}` "
    )
    query_job = client.query(
        query,
        # Location must match that of the dataset(s) referenced in the query.
        location="US",
    )  # API request - starts the query
    
    
    results = query_job.result().to_dataframe()  # Wait for query to complete.
    return(
            "Loaded {} rows in {}.".format(
                results.contador[0],table_id
            )
        )

