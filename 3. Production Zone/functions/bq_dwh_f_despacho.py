# -*- coding: utf-8 -*-

def bq_job_dwh_f_despacho(table_id,bq_client):
    
    client=bq_client
    
    bq_query = (
                """
                
                --Created at 2021-07-14 by KV
                
                IF NOT EXISTS (SELECT 1 FROM dwh.__TABLES__ WHERE table_id='f_despacho')
                    THEN 
                        CREATE TABLE dwh.f_despacho AS
                            SELECT * FROM stage.f_despacho;
                    ELSE 
                        BEGIN
                        ----En esta parte se crea la carga incremental
                        DECLARE statement STRING;
                        SET statement = (
                        WITH  
                        table1_columns AS ( --Aqui se obtiene la lista de columnas de stage
                            SELECT column FROM (SELECT * FROM `stage.f_despacho` LIMIT 1) t,
                            UNNEST(REGEXP_EXTRACT_ALL(TRIM(TO_JSON_STRING(t), '{}'), r'"([^"]*)":')) column
                            ), 
                        table2_columns AS ( --Aqui se obtiene la lista de columnas de dwh
                            SELECT column FROM (SELECT * FROM `dwh.f_despacho` LIMIT 1) t,
                            UNNEST(REGEXP_EXTRACT_ALL(TRIM(TO_JSON_STRING(t), '{}'), r'"([^"]*)":')) column
                            ), 
                        all_columns AS ( --Se juntan ambas listas
                            SELECT column FROM table1_columns UNION DISTINCT SELECT column FROM table2_columns
                            ),
                        final_columns AS (
                            SELECT
                            column as column_name,
                            (SELECT column as column_t1 FROM table1_columns t1 WHERE ac.column=t1.column) as column_t1,
                            (SELECT column as column_t2 FROM table2_columns t2 WHERE ac.column=t2.column) as column_t2
                            FROM all_columns ac
                            ),
                        print_table AS ( --Se les da formato a la tabla con la consulta SQL
                            SELECT
                            print_t1,
                            print_t2,
                            ROW_NUMBER() OVER(partition BY scalar) as nro_row
                            FROM(
                                SELECT 
                                IFNULL(column_t1,'NULL as '||column_name) as print_t1,
                                IFNULL(column_t2,'NULL as '||column_name) as print_t2,
                                1 as scalar
                                FROM final_columns)
                            )
                        SELECT (
                            SELECT 
                                'CREATE OR REPLACE TABLE dwh.f_despacho AS SELECT ' ||
                                --STRING_AGG permite concatenar los valores de tabla consulta SQL
                                 STRING_AGG(p.print_t1, ', 'order by nro_row) ||
                                ' FROM `stage.f_despacho` UNION ALL '
                            FROM print_table p
                            ) || (
                            SELECT 
                                'SELECT ' || 
                                STRING_AGG(p.print_t2, ', 'order by nro_row) || 
                                --En la parte WHERE NT EXISTS(), se formula la llave de la Carga Incremental
                                ' FROM `dwh.f_despacho` d WHERE NOT EXISTS(SELECT pkTiempo FROM stage.f_despacho s WHERE d.pkTiempo=s.pkTiempo)'
                            FROM print_table p
                            ) 
                        );
                        EXECUTE IMMEDIATE statement;
                        END;  
                END IF;
                    
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