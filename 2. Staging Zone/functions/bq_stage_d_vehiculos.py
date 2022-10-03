# -*- coding: utf-8 -*-

def bq_job_stage_d_vehiculos(table_id,bq_client):
    
    client=bq_client
    
    bq_query = (
                f"""
                
                CREATE OR REPLACE TABLE stage.d_vehiculos AS
                
                  SELECT
                    idPlaca,
                    propietario as nombPropietarioVehiculo,
                
                    if(rznSocial like '%MOY%','20297421035',rznSocial)
                        as pkPropietarioVehiculo,
                
                    estado as estadoVehiculo,
                    IF(considerarPropio='Si',1,0) as flgConsiderarPropioVehiculo,
                    IF((dimLargo)>0,dimLargo,Null) AS dimLargoVehiculo,
                    IF((dimAlto)>0,dimAlto,Null) AS dimAltoVehiculo,
                    IF((dimAncho)>0,dimAncho,Null) AS dimAnchoVehiculo,
                    IF((dimInteriorLargo)>0,dimInteriorLargo,Null) AS dimInteriorLargoVehiculo,
                    IF((dimInteriorAncho)>0,dimInteriorAncho,Null) AS dimInteriorAnchoVehiculo,
                    IF((dimInteriorAlto)>0,dimInteriorAlto,Null) AS dimInteriorAltoVehiculo,
                    IF((m3Facturable)>0,m3Facturable,Null) AS capFactVehiculo,
                    IF((capCombustible)>0,capCombustible,Null) AS capCombustibleVehiculo,
                    IF((rendimiento)>0,rendimiento,Null) AS rendEsperadoCombustibleVehiculo,
                    IF(LENGTH(checkAlertas)>0,checkAlertas,Null) AS flgCheckAlertasVehiculo,
                    IF(LENGTH(marca)>0,UPPER(marca),Null) AS marcaVehiculo,
                    IF(LENGTH(modelo)>0,upper(modelo),Null) AS modeloVehiculo,
                    IF((anioFabricacion)>100,anioFabricacion,Null) AS anioFabricacionVehiculo,
                    IF(LENGTH(color)>0,upper(color),Null) AS colorVehiculo,
                    IF(LENGTH(carroceria)>0,upper(carroceria),Null) AS carroceriaVehiculo,
                    IF(LENGTH(clase)>0,upper(clase),Null) AS claseVehiculo,
                    IF((cilindros)>0,cilindros,Null) AS cilindrosVehiculo,
                    IF(LENGTH(cc)>0,upper(cc),Null) AS ccVehiculo,
                    IF((hp)>0,hp,Null) AS hpVehiculo,
                    IF((nroRuedas)>0,nroRuedas,Null) AS nroRuedasVehiculo,
                    IF(LENGTH(llantas)>0,upper(llantas),Null) AS llantasVehiculo,
                    --pulg,  No sé a qué hace referencia
                    pesoSeco as pesoSecoVehiculo,
                    pesoUtil as pesoUtilVehiculo,
                    pesoBruto as pesoBrutoVehiculo,
                    pesoUtilReal as pesoUtilRealVehiculo,
                    lower(usuario) as usuarioCreacVehiculo,
                    fchCreacion as fchCreacVehiculo,
                    usuarioUltimoCambio as usuarioEdicVehiculo,
                    fchCarga,
                  FROM
                    landing.vehiculo
                  WHERE
                      fchCarga= --Get the last modified table
                                  (SELECT DISTINCT fchCarga 
                                  FROM landing.vehiculo 
                                  ORDER BY fchCarga DESC 
                                  LIMIT 1)
                  AND rznSocial not in ('moy3','BCP','TERCEROS','JURY SISI AMAYA SANT')          
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