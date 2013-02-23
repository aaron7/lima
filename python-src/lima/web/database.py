'''
Created on 1 Feb 2013

@author: aaron
'''

import happybase
import psycopg2

class PostgresDB():
    
    def __init__(self):
        conn_string = "host='localhost' port='5433' dbname='lima' user='postgres' password='lima'"
        self.conn = psycopg2.connect(conn_string)
        print "Connection made to PostgreSQL"
        
    def executeQuery(self, queryString):
        cursor = self.conn.cursor()
        cursor.execute(queryString)
        return cursor #return the cursor to allow full control
    
class HBaseDB():
    
    def __init__(self):
        self.conn = happybase.Connection('localhost', autoconnect=False, port=9092)
        self.conn.open()
        print "Connection made to HBase"

    def getTables(self):
        return self.conn.tables()
    
    def getRow(self, table, key):
        return table.row(key) #key as a string
    
    def getRows(self, table, keys):
        return table.rows(keys); #key as a list of strings
            
    def getInstance(self):
        return self.conn
    
    def createTable(self, name, families):
        self.conn.create_table(name,families) #name as string, families as mapping
        
    def scan(self):
        table = self.conn.table('statistics')
        for key, data in table.scan():
            print key, data
        
        