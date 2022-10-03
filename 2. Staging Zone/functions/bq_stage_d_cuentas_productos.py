# -*- coding: utf-8 -*-

def bq_job_stage_d_cuentas_productos(table_id,bq_client):
    
    client=bq_client
    
    bq_query = (
                f"""
                
                CREATE OR REPLACE TABLE stage.d_cuentas_productos AS

                SELECT 
                    idProducto as pkProducto,
                    p.idCliente as pkCliente,
                    p.correlativo as pkCuenta,
                    nombreCuenta,
                    nombProducto as nombreProducto,
                    m3Facturable m3FacturableProd,
                    puntos as nroPuntosProd,
                    idZona as pkZona,
                    zona as zonaProd,
                    tipoProducto as tipoProd,
                    estadoProducto as estadoProd,
                    precioServ as precioServProd,
                    kmEsperado as kmEspProd,
                    tolerKmEsperado as tolerKmEspProd,
                    valKmAdic as valKmAdicProd,
                    TIME_ADD(TIME "00:00:00",INTERVAL CAST(CAST(hrasNormales AS FLOAT64) AS INT64) SECOND) AS hrasNormalProd,
                    tolerHrasNormales as tolerHrasNorProd,
                    valHraAdic as valHraAdicProd,
                    TIME_ADD(TIME "00:00:00",INTERVAL CAST(CAST(hraIniEsperado AS FLOAT64) AS INT64) SECOND) AS hraIniEspProd,
                    tolerHraIniEsperado as tolerHraIniEspProd,
                    valAdicHraIniEsper as valAdicHraIniEspProd,
                    TIME_ADD(TIME "00:00:00",INTERVAL CAST(CAST(hraFinEsperado AS FLOAT64) AS INT64) SECOND) AS hraFinEspProd,
                    tolerHraFinEsperado as tolerHraFinEspProd,
                    valAdicHraFinEsper as valAdicHraFinEspProd,
                    nroAuxiliares as nroAuxiliaresProd,
                    valAuxiliarAdic as valAuxiliarAdicProd,
                    cobrarPeaje as cobrarPeajeProd,
                    cobrarRecojoDevol as cobrarRecojoDevolProd,
                    valConductor as valConductorProd,
                    TIME_ADD(TIME "00:00:00",INTERVAL CAST(CAST(hraNormalConductor AS FLOAT64) AS INT64) SECOND) AS hraNormalConductorProd,
                    tolerHraCond as tolerHraCondProd,
                    valHraAdicCond as valHraAdicCondProd,
                    valAuxiliar as valAuxiliarProd,
                    TIME_ADD(TIME "00:00:00",INTERVAL CAST(CAST(hraNormalAux AS FLOAT64) AS INT64) SECOND) AS hraNormalAuxProd,
                    tolerHraAux as tolerHraAuxProd,
                    valHraAdicAux as valHraAdicAuxProd,
                    usoMaster as usoMasterProd,
                    valUnidTercCCond as valUnidTercCCondProd,
                    valUnidTercSCond as valUnidTercSCondProd,
                    TIME_ADD(TIME "00:00:00",INTERVAL CAST(CAST(hrasNormalTerc AS FLOAT64) AS INT64) SECOND) AS hrasNormalTercProd,
                    tolerHrasNormalTerc as tolerHrasNormalTercProd,
                    valHraExtraTerc as valHraExtraTercProd,
                    valKmAdicTerc as valKmAdicTercProd,
                    valAuxTercero as valAuxTerceroProd,
                    contarDespacho,
                    superCuenta,
                    creacUsuario as creacUsuarioProd,
                    creacFch as creacFchProd,
                    editUsuario as editUsuarioProd,
                    editFch as editFchProd,
                    p.fchCarga
                FROM landing.cliente_producto p
                LEFT JOIN ( SELECT nombreCuenta,idCliente,correlativo FROM landing.cliente_cuenta
                            WHERE
                                fchCarga= --Get the last modified table
                                            (SELECT DISTINCT fchCarga 
                                            FROM landing.cliente_cuenta 
                                            ORDER BY fchCarga DESC 
                                            LIMIT 1)) c 
                    ON p.idCliente||p.correlativo=c.idCliente||c.correlativo
                WHERE
                    fchCarga= --Get the last modified table
                                (SELECT DISTINCT fchCarga 
                                FROM landing.cliente_producto 
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