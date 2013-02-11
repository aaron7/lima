'''
Created on 1 Feb 2013

@author: aaron
'''

import happybase

connection = happybase.Connection('127.0.0.1', autoconnect=False)
connection.open()
print "Connection made to HBase"

def getTables():
    return connection.tables()
    
def getRow(table,key):
    return table.row(key) #key as a string
    
def getRows(table,keys):
    return table.rows(keys); #key as a list of strings
            
def getInstance():
    return connection
    
def createTable(name,families):
    connection.create_table(name,families) #name as string, families as mapping