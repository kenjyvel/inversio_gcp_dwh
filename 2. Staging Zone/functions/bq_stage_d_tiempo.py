# -*- coding: utf-8 -*-

def bq_job_stage_d_tiempo(table_id,bq_client):
    
    client=bq_client
    
    bq_query = (
                f"""
                
                --Created at 2021-07-15 by KV
                --Added fchCarga 2021-07-16 by KV
                DECLARE current_date DATE;
                SET current_date=CURRENT_DATE('America/Lima');
                
                CREATE OR REPLACE TABLE stage.d_tiempo AS
                    
                    SELECT
                        *,
                        IF(
                            numAnio = EXTRACT(YEAR FROM current_date),
                            1,0
                        ) as anioActivo,
                        IF(
                            numAnio*100+numMes = EXTRACT(YEAR FROM current_date)*100+EXTRACT(MONTH FROM current_date),
                            1,0
                        ) as mesActivo,
                        IF(
                            CAST(pkSemanaISO AS INT64) = EXTRACT(ISOYEAR FROM current_date)*100+EXTRACT(ISOWEEK FROM current_date),
                            1,0
                        ) as semActiva,
                        IF(
                            CAST(pkSemanaISO AS INT64) = EXTRACT(ISOYEAR FROM (current_date-7))*100+EXTRACT(ISOWEEK FROM (current_date-7)),
                            1,0
                        ) as semAntActiva,
                        IF(
                            CAST(pkSemanaISO AS INT64) >= EXTRACT(ISOYEAR FROM (current_date-7))*100+EXTRACT(ISOWEEK FROM (current_date-7))
                            and
                            CAST(pkSemanaISO AS INT64) <= EXTRACT(ISOYEAR FROM (current_date))*100+EXTRACT(ISOWEEK FROM (current_date))
                            ,
                            1,0
                        ) as ult2SemActiva,
                
                        IF(
                            CAST(pkSemanaISO AS INT64) >= EXTRACT(ISOYEAR FROM (current_date-7*3))*100+EXTRACT(ISOWEEK FROM (current_date-7*3))
                            and
                            CAST(pkSemanaISO AS INT64) <= EXTRACT(ISOYEAR FROM (current_date))*100+EXTRACT(ISOWEEK FROM (current_date))
                            ,
                            1,0
                        ) as ult4SemActiva,
                
                        TIMESTAMP(CURRENT_DATETIME('America/Lima')) AS fchCarga,
                
                        --evolutivo_activo,
                    FROM(
                        SELECT
                            pk_tiempo as pkTiempo,
                            date(num_anio,num_mes,num_dia_mes) as fchTiempo,
                            num_anio as numAnio,
                            num_semestre as numSemestre,
                            num_cuatrimes as numCuatrimestre,
                            num_trimestre as numTrimestre,
                            num_mes as numMes,
                            num_sem_anio as numSem,
                            num_dia_semana as numDiaSemana,
                            num_dia_mes as numDiaMes,
                            num_dia_anio as numDiaAnio,
                            ctd_dias_anio as ctdDiasAnio,
                            ctd_dias_mes as ctdDiasMes,
                            ctd_dias_semana as ctdDiasSemana,
                            --flg_laborable as flgLaborable,
                            --flg_fin_semana as flgFinSemana,
                            --flg_feriado as flgFeriado,
                            --flg_festivo as flgFestivo,
                            --des_feriado as desFeriado,
                            --des_festivo as desFestivo,
                            pk_tiempo_ant as pkTiempoAnt,
                            pk_tiempo_ant2 as pkTiempoAnt2,
                            num_sem_anio_iso as pkSemanaISO,
                            aniomes AS desAnioMes,
                            des_mes AS desMes,
                            des_dia AS desDia,
                            sem_anio AS desAnioSemana,
                        FROM landing.tiempo)             
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