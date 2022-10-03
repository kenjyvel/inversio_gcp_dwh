from function.mysql_connection import mysql_open_conn
from function.mysql_connection import mysql_close_conn
from function.bigquery_connection import bq_conn

#Fact Tables
from function.t_despacho import t_despacho_to_bq
from function.t_despacho_personal import t_despacho_personal_to_bq
from function.t_despacho_puntos import t_despacho_puntos_to_bq
from function.t_despacho_puntos_productos import t_despacho_puntos_productos_to_bq
from function.t_programacion_despacho import t_programacion_despacho_to_bq
from function.t_programacion_despacho_personal import t_programacion_personal_to_bq
from function.t_combustible import t_combustible_to_bq
from function.t_despacho_cobranza import t_despacho_cobranza_to_bq
from function.t_documento_cobranza import t_doc_cobranza_to_bq


#Dimension Tables
from function.t_clientes import t_clientes_to_bq
from function.t_cliente_cuenta import t_cliente_cuenta_to_bq
from function.t_cliente_producto import t_cliente_producto_to_bq
from function.t_trabajador import t_trabajador_to_bq
from function.t_vehiculo import t_vehiculo_to_bq
from function.t_usuario_incidencias import t_usuarios_incidencias_to_bq