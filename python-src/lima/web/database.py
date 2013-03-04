'''
Lima Web UI
'''

import happybase
import psycopg2
import re

"""
This class contains all the functions to handle PostgreSQL
"""
class PostgresDB():
    
    """
    Make the connection
    """
    def __init__(self):
        conn_string = "host='localhost' port='5433' dbname='lima' user='postgres' password='lima'"
        self.conn = psycopg2.connect(conn_string)
        print "Connection made to PostgreSQL"
        
    def executeQuery(self, queryString):
        cursor = self.conn.cursor()
        cursor.execute(queryString)
        return cursor #return the cursor to allow full control
    
    def commit(self):
        self.conn.commit();

"""
This class contains all the functions to handle HBase
"""
class HBaseDB():
    
    """
    Make the connection
    """
    def __init__(self):
        self.conn = happybase.Connection('localhost', autoconnect=False, port=9091)
        self.conn.open()
        print "Connection made to HBase"

    def getTables(self):
        return self.conn.tables()
    
    def getRow(self, table, key):
        return table.row(key) #key as a string
    
    def getRows(self, table, keys):
        return table.rows(keys); #key as a list of strings
            
    def getTable(self, tableName):
        return self.conn.table(tableName)
    
    def getInstance(self):
        return self.conn #return the instance of the connection for full control
    
    def createTable(self, name, families):
        self.conn.create_table(name,families) #name as string, families as mapping
        
    def toLong(self,hexString):
        hexString = bytearray(hexString)[0:8] #convert to byte array and take the right bits for a long
        hexString.reverse()
        result = 0L
        for index, b in enumerate(hexString):
            result = result + (b << (index*8))
        return result
    
    def toInt(self,hexString):
        hexString = bytearray(hexString)[0:4] #convert to byte array and take the right bits for a long
        hexString.reverse()
        result = 0
        for index, b in enumerate(hexString):
            result = result + (b << (index*8))
        return result
    
    def toString(self,hexString):
        hexString = hexString[1:]
        return re.sub("\x00", "", hexString)
        