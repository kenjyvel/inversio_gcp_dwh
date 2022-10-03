# -*- coding: utf-8 -*-
"""
Created on Tue Jul 13 09:48:45 2021

@author: KV
"""

from functions import *

def handler(request):
       
    #Open BigQuery Connection
    
    client=bq_conn()
    
    #Poblar tabla despacho
    table_id = "primal-stock-314922.dwh.f_despacho"
    print(bq_job_dwh_f_despacho(table_id,client))
    
    #Poblar tabla despacho puntos
    table_id = "primal-stock-314922.dwh.f_despacho_puntos"
    print(bq_job_dwh_f_despacho_puntos(table_id,client))
    
    #Poblar tabla despacho productos
    table_id = "primal-stock-314922.dwh.f_despacho_productos"
    print(bq_job_dwh_f_despacho_productos(table_id,client))
    
    #Poblar tabla programacion
    table_id = "primal-stock-314922.dwh.f_programacion"
    print(bq_job_dwh_f_programacion(table_id,client))    
    
    #Poblar tabla abastecimientos
    table_id = "primal-stock-314922.dwh.f_abastecimiento"
    print(bq_job_dwh_f_abastecimiento(table_id,client))    

    #Poblar tabla clientes
    table_id = "primal-stock-314922.dwh.d_clientes"
    print(bq_job_dwh_d_clientes(table_id,client))
    
    #Poblar tabla vehiculos
    table_id = "primal-stock-314922.dwh.d_vehiculos"
    print(bq_job_dwh_d_vehiculos(table_id,client))

    #Poblar tabla cuentas productos
    table_id = "primal-stock-314922.dwh.d_cuentas_productos"
    print(bq_job_dwh_d_cuentas_productos(table_id,client))

    #Poblar tabla tiempo
    table_id = "primal-stock-314922.dwh.d_tiempo"
    print(bq_job_dwh_d_tiempo(table_id,client))    
      
    return 'Prod DWH Load - Successful'
    
#handler(1)