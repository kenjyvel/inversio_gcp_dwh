# -*- coding: utf-8 -*-

def bq_job_stage_f_despacho_puntos(table_id,bq_client):
    
    client=bq_client
    
    bq_query = (
                f"""
                
                CREATE OR REPLACE TABLE stage.f_despacho_puntos AS
                
                  SELECT
                    idPunto as pkPunto,
                    a.idProgramacion as pkProgramacion,
                    pkDespacho,
                    CAST(TRUNC(pkDespacho/1000) AS INT64) AS pkTiempo,
                    --idRuta,
                    ordenPunto,
                    --correlPunto,
                    IF(LENGTH(tipoPunto)>0,tipoPunto,'Descarga') AS tipoPunto,
                    IF(LENGTH(nombComprador)>0,nombComprador,Null) AS nombDestinatario,
                    --idDistrito,
                    IF(LENGTH(distrito)>0,distrito,Null) as distritoPunto,
                    IF(LENGTH(provincia)>0,provincia,Null) as provinciaPunto,
                    IF(LENGTH(direccion)>0,direccion,Null) as direccionPunto,
                    IF(LENGTH(guiaCliente)>0,guiaCliente,Null) as guiaClientePunto,
                    estado as estadoPunto,
                    subEstado as subEstadoPunto,
                    IF(CAST(hraCita AS FLOAT64)=0,NULL,stage.tsFechaHora(fchDespacho,hraCita))
                      as fchCita,
                    IF(CAST(hraLlegada AS FLOAT64)=0,NULL,stage.tsFechaHora(fchDespacho,hraLlegada))
                      as fchLlegada,
                    IF(CAST(hraSalida AS FLOAT64)=0,NULL,stage.tsFechaHora(fchDespacho,hraSalida))
                      as fchSalida,
                    --longitud,
                    --latitud,
                    --longRecib,
                    --latRecib,
                    IF(LENGTH(observacion)>0,observacion,Null) as observPunto,
                    --referencia,
                    --telfReferencia,
                    idCarga as codCargaPunto,
                    IF(LENGTH(observCargaPunto)>0,observCargaPunto,Null) as observCargaPunto,
                    creacModo as creacModoPunto,
                    creacUsuario as usuarioCreacPunto,
                    creacFch as fchCreacPunto,
                      IF(LENGTH(editaModo)>0,editaModo,Null) as editaModoPunto,
                    IF(LENGTH(editaUsuario)>0,editaUsuario,Null) as editaUsuarioPunto,
                    TIMESTAMP(editaFch) as editaFchPunto,
                    fchCarga,
                  FROM
                    landing.despacho_puntos a
                  LEFT JOIN
                    (SELECT
                      stage.pkDespacho(fchDespacho,correlativo) AS pkDespacho,
                      idProgramacion,
                      fchDespacho,
                      FROM
                      landing.despacho
                      WHERE fchCarga= --Get the last modified table
                                  (SELECT DISTINCT fchCarga 
                                  FROM landing.despacho 
                                  ORDER BY fchCarga DESC 
                                  LIMIT 1)) b
                  ON a.idProgramacion=b.idProgramacion
                  WHERE fchCarga= --Get the last modified table
                                  (SELECT DISTINCT fchCarga 
                                  FROM landing.despacho_puntos 
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