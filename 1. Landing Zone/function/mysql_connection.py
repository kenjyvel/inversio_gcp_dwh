# -*- coding: utf-8 -*-

import MySQLdb

   
#database variables
host = '***'
user = '***'
password = '***'
database = '***'
char_set='utf8'

def mysql_open_conn():
    conn = MySQLdb.connect(host=host,
                           user=user,
                           passwd=password,
                           db=database,
                           charset=char_set)
    return conn
    
def mysql_close_conn():
    conn = MySQLdb.connect(host=host,
                           user=user,
                           passwd=password,
                           db=database,
                           charset=char_set)
    return conn.close()


