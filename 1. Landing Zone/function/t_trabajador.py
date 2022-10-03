from google.cloud import bigquery
from datetime import date, timedelta, datetime, timezone
import pandas as pd


def t_trabajador_to_bq(mysql_conn,table_id,bq_client):
    
    conn=mysql_conn
    client=bq_client
      
    datetime_load=datetime.utcnow().replace(tzinfo=timezone.utc)
    timestamp_loaded=int(datetime.timestamp(datetime_load)*1000000)

        
    query = """SELECT
                idTrabajador,
                tipoDocTrab,
                nroDocTrab,
                fchCaducidad,
                tipoTrabajador,
                categTrabajador,
                ruc,
                modoSueldo,
                estadoTrabajador,
                apPaterno,
                apMaterno,
                nombres,
                fchNacimiento,
                telfMovil,
                dirCalleyNro,
                dirDistrito,
                dirProvincia,
                dirDepartamento,
                dirTelefono,
                dirObservacion,
                dirNro,
                dirUrb,
                telfAdicional,
                telfReferencia,
                telfRefContacto,
                telfRefParentesco,
                sexo,
                eMail,
                estadoCivil,
                gradoInstruccion,
                licenciaNro,
                licenciaVigencia,
                licenciaCategoria,
                modoContratacion,
                remuneracionBasica,
                asignacionFamiliar,
                minimoSemanal,
                cajaChica,
                renta5ta,
                diasVacacAnual,
                manejaLiquidacion,
                fondoGarantia,
                esMaster,
                precioMaster,
                deseaDscto,
                deseaDsctoOnp,
                entidadPension,
                idPoliza,
                tipoComision,
                asumeEmpresa,
                descontarSeguro,
                descontarComisionAfp,
                descontarAporteAfp,
                cuspp,
                codTrabajador,
                movilidadAsignadaMes,
                formaPago,
                usuario,
                fchCreacion,
                usuarioUltimoCambio,
                fchUltimoCambio,
                estadoQuincena,
                fchDesmarcaEstadoQ
                FROM trabajador """
                
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

