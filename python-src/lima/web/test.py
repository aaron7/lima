'''
Created on 15 Feb 2013

@author: aaron
'''

from database import PostgresDB, HBaseDB
from events import Events

testDB = PostgresDB()

testEventsConnection = Events(testDB)

print testEventsConnection.getAllEvents()



#happyDB = HBaseDB()

#happyDB.scan()