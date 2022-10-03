from function import *

def handler(request):
    
    #Open MySQL Connection
    
    mysql_conn=mysql_open_conn()
    
    #Open BigQuery Connection
    
    client=bq_conn()
    
    #Poblar tabla despacho
    table_id_despacho = "primal-stock-314922.landing.despacho"
    print(t_despacho_to_bq(mysql_conn,table_id_despacho,client))
    
    #Poblar tabla despacho personal
    table_id_despacho_personal = "primal-stock-314922.landing.despacho_personal"
    print(t_despacho_personal_to_bq(mysql_conn,table_id_despacho_personal,client))
    
    #Poblar tabla programacion
    table_id_programacion_despacho = "primal-stock-314922.landing.programacion_despacho"
    print(t_programacion_despacho_to_bq(mysql_conn,table_id_programacion_despacho,client))
       
    #Poblar tabla programacion personal
    table_id_programacion_personal = "primal-stock-314922.landing.programacion_personal"
    print(t_programacion_personal_to_bq(mysql_conn,table_id_programacion_personal,client))
    
    #Poblar tabla combustible
    table_id_combustible = "primal-stock-314922.landing.combustible"
    print(t_combustible_to_bq(mysql_conn,table_id_combustible,client))

    #Poblar tabla despacho puntos
    table_id_despacho_puntos = "primal-stock-314922.landing.despacho_puntos"
    print(t_despacho_puntos_to_bq(mysql_conn,table_id_despacho_puntos,client))
    
    #Poblar tabla despacho puntos
    table_id_despacho_puntos_productos = "primal-stock-314922.landing.despacho_puntos_productos"
    print(t_despacho_puntos_productos_to_bq(mysql_conn,table_id_despacho_puntos_productos,client))
    
    #Poblar tabla despacho cobranza
    table_id_despacho_cobranza = "primal-stock-314922.landing.despacho_cobranza"
    print(t_despacho_cobranza_to_bq(mysql_conn,table_id_despacho_cobranza,client))
    
    #Poblar tabla doc cobranza
    table_id_doc_cobranza = "primal-stock-314922.landing.doc_cobranza"
    print(t_doc_cobranza_to_bq(mysql_conn,table_id_doc_cobranza,client))
    
    #Poblar tabla clientes
    table_id_clientes = "primal-stock-314922.landing.clientes"
    print(t_clientes_to_bq(mysql_conn,table_id_clientes,client))
    
    #Poblar tabla cliente cuenta
    table_id_cliente_cuenta = "primal-stock-314922.landing.cliente_cuenta"
    print(t_cliente_cuenta_to_bq(mysql_conn,table_id_cliente_cuenta,client))
    
    #Poblar tabla cliente producto
    table_id_cliente_producto = "primal-stock-314922.landing.cliente_producto"
    print(t_cliente_producto_to_bq(mysql_conn,table_id_cliente_producto,client))
    
    #Poblar tabla trabajador
    table_id_trabajador = "primal-stock-314922.landing.trabajador"
    print(t_trabajador_to_bq(mysql_conn,table_id_trabajador,client))
    
    #Poblar tabla vehiculo
    table_id_vehiculo = "primal-stock-314922.landing.vehiculo"
    print(t_vehiculo_to_bq(mysql_conn,table_id_vehiculo,client))
    
    #Poblar tabla usuarios incidencias
    table_id_usuario_incidencia = "primal-stock-314922.landing.usuario_incidencia"
    print(t_usuarios_incidencias_to_bq(mysql_conn,table_id_usuario_incidencia,client))
       
    #Close MySQL Connection
    mysql_close_conn()
    
    return 'Landing Load - Successful'
    
handler(1)
