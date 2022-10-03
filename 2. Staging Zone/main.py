# -*- coding: utf-8 -*-
"""
Created on Tue Jul 13 09:48:45 2021

@author: User
"""

from functions import *

def handler(request):
       
    #Open BigQuery Connection
    
    client=bq_conn()
    
    #Poblar tabla despacho
    table_id = "primal-stock-314922.stage.f_despacho"
    print(bq_job_stage_f_despacho(table_id,client))
    
    #Poblar tabla despacho puntos
    table_id = "primal-stock-314922.stage.f_despacho_puntos"
    print(bq_job_stage_f_despacho_puntos(table_id,client))
    
    #Poblar tabla despacho productos
    table_id = "primal-stock-314922.stage.f_despacho_productos"
    print(bq_job_stage_f_despacho_productos(table_id,client))
    
    #Poblar tabla programacion
    table_id = "primal-stock-314922.stage.f_programacion"
    print(bq_job_stage_f_programacion(table_id,client))
    
    #Poblar tabla cuentas abastecimientos
    table_id = "primal-stock-314922.stage.f_abastecimiento"
    print(bq_job_stage_f_abastecimientos(table_id,client))
    
    #Poblar tabla clientes
    table_id = "primal-stock-314922.stage.d_clientes"
    print(bq_job_stage_d_clientes(table_id,client))
    
    #Poblar tabla vehiculos
    table_id = "primal-stock-314922.stage.d_vehiculos"
    print(bq_job_stage_d_vehiculos(table_id,client))

    #Poblar tabla cuentas productos
    table_id = "primal-stock-314922.stage.d_cuentas_productos"
    print(bq_job_stage_d_cuentas_productos(table_id,client))
    

    return 'Stage Load - Successful'
    
#handler(1)