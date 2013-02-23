'''
Created on 14 Feb 2013

@author: aaron
'''

import json

eventFields = "\"eventID\",\"routerIP\",\"type\",\"status\",\"message\",\"startTime\",\"endTime\",\"createTS\""

class EventsHandler():
    
    """get the database connections, get all of the events in the DB and put them in the events list
    """
    
    def __init__(self,pgDB,red):
        self.pgDB = pgDB #set the postgreSQL connection
        self.red = red #get the redis instance
        
        curEvents = self.pgDB.executeQuery("SELECT "+eventFields+" FROM event ORDER BY \"eventID\" ASC")
        self.eventsList = curEvents.fetchall()
        self.lastEventID = self.eventsList[len(self.eventsList)-1][0] #update last event ID
    
    """returns the events list in JSON
    """
    def getEventsList(self):
        return self.eventsList
    
    """get new events, publish the new events, update last event ID and append to the Events List
    """
    def checkNewEvents(self):
        curNewEvents = self.pgDB.executeQuery("SELECT "+eventFields+" FROM event WHERE \"eventID\" > '"+str(self.lastEventID)+"'")
        if curNewEvents.rowcount > 0 :
            newEvents = curNewEvents.fetchall()
            self.red.publish("events",json.dumps(newEvents)) #send the updates to the stream
            self.lastEventID = newEvents[len(newEvents)-1][0] #update last event ID
            self.eventsList.extend(newEvents)
            
            
    #TODO: PRUNE OLD EVENTS FROM THE TABLE
    
    
    
    
    