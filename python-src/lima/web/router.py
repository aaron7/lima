'''
Created on 23 Feb 2013

@author: aaron
'''

import json

routerFields = "\"routerIP\",\"lastSeen\",\"flowsPH\",\"packetsPH\",\"bytesPH\""

class RouterHandler():
    
    """get the database connections, get all of the routers in the DB and put them in the routers list
    """
    
    def __init__(self,pgDB,red):
        self.pgDB = pgDB #set the postgreSQL connection
        self.red = red #get the redis instance
        
        self.routersList = self.getRouters()
    
    """returns the events list in JSON
    """
    def getRoutersList(self):
        return self.routersList
    
    def getRouters(self):
        curEvents = self.pgDB.executeQuery("SELECT "+routerFields+" FROM router ORDER BY \"routerIP\" ASC")
        return curEvents.fetchall()
    
    """handle router updates using differences
    """
    def checkRouterUpdates(self):
        #get the difference between the local copy of the table and database itself
        tempList = self.getRouters()
        tempSet = set(tempList)
        updatesSet = tempSet.difference(set(self.routersList))
        
        if len(updatesSet) > 0 :
            #if our local copy is out of date push all changes and update our local copy
            self.red.publish("routerUpdates",json.dumps(sorted(list(updatesSet))))
            self.routersList = tempList #update our local copy
            
    #def getEventsOverview(self):
        
        
        
        
        
        
        