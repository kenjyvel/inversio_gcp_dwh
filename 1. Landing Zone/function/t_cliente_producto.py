from google.cloud import bigquery
from datetime import date, timedelta, datetime, timezone
import pandas as pd


def t_cliente_producto_to_bq(mysql_conn,table_id,bq_client):
    
    conn=mysql_conn
    client=bq_client
      
    datetime_load=datetime.utcnow().replace(tzinfo=timezone.utc)
    timestamp_loaded=int(datetime.timestamp(datetime_load)*1000000)

        
    query = """SELECT
                idProducto,
                idCliente,
                correlativo,
                nombProducto,
                m3Facturable,
                puntos,
                idZona,
                zona,
                tipoProducto,
                estadoProducto,
                precioServ,
                kmEsperado,
                tolerKmEsperado,
                valKmAdic,
                hrasNormales,
                tolerHrasNormales,
                valHraAdic,
                hraIniEsperado,
                tolerHraIniEsperado,
                valAdicHraIniEsper,
                hraFinEsperado,
                tolerHraFinEsperado,
                valAdicHraFinEsper,
                nroAuxiliares,
                valAuxiliarAdic,
                cobrarPeaje,
                cobrarRecojoDevol,
                valConductor,
                hraNormalConductor,
                tolerHraCond,
                valHraAdicCond,
                valAuxiliar,
                hraNormalAux,
                tolerHraAux,
                valHraAdicAux,
                usoMaster,
                valUnidTercCCond,
                valUnidTercSCond,
                hrasNormalTerc,
                tolerHrasNormalTerc,
                valHraExtraTerc,
                valKmAdicTerc,
                valAuxTercero,
                contarDespacho,
                superCuenta,
                creacUsuario,
                creacFch,
                editUsuario,
                editFch
                FROM clientecuentaproducto """
                
    df=pd.read_sql_query(query, conn)

    df["hrasNormales"]=df["hrasNormales"].dt.total_seconds().round().astype(str)
    df["tolerHrasNormales"]=df["tolerHrasNormales"].dt.total_seconds().round().astype(str)
    df["hraIniEsperado"]=df["hraIniEsperado"].dt.total_seconds().round().astype(str)
    df["tolerHraIniEsperado"]=df["tolerHraIniEsperado"].dt.total_seconds().round().astype(str)
    df["hraFinEsperado"]=df["hraFinEsperado"].dt.total_seconds().round().astype(str)
    df["tolerHraFinEsperado"]=df["tolerHraFinEsperado"].dt.total_seconds().round().astype(str)
    df["hraNormalConductor"]=df["hraNormalConductor"].dt.total_seconds().round().astype(str)
    df["tolerHraCond"]=df["tolerHraCond"].dt.total_seconds().round().astype(str)
    df["hraNormalAux"]=df["hraNormalAux"].dt.total_seconds().round().astype(str)
    df["tolerHraAux"]=df["tolerHraAux"].dt.total_seconds().round().astype(str)
    df["hrasNormalTerc"]=df["hrasNormalTerc"].dt.total_seconds().round().astype(str)
    df["tolerHrasNormalTerc"]=df["tolerHrasNormalTerc"].dt.total_seconds().round().astype(str)
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

