'''
Lima Web UI
'''

import json

#define fields for SQL queries
eventFields = "\"eventID\",\"routerIP\",\"type\",\"status\",\"message\",\"startTime\",\"endTime\",\"createTS\""

"""
This class contains all the functions to handle the information for all of the events
"""
class EventsHandler():
    
    """
    Initial startup function
    
    Set up databases, create cache and get all of the routers
    """
    def __init__(self,pgDB,red):
        self.pgDB = pgDB
        self.red = red
        
        #get all fo the events and store them in the class
        curEvents = self.pgDB.executeQuery("SELECT "+eventFields+" FROM event ORDER BY \"eventID\" ASC")
        self.eventsList = curEvents.fetchall()
        self.lastEventID = self.eventsList[len(self.eventsList)-1][0] #update last event ID
    
    """
    Returns the events list
    """
    def getEventsList(self):
        return self.eventsList
    
    """
    Get new events, publish the new events, update last event ID and append to the Events List
    """
    def checkNewEvents(self):
        curNewEvents = self.pgDB.executeQuery("SELECT "+eventFields+" FROM event WHERE \"eventID\" > '"+str(self.lastEventID)+"'")
        if curNewEvents.rowcount > 0 :
            newEvents = curNewEvents.fetchall()
            self.red.publish("events",json.dumps(newEvents)) #send the updates to the stream
            self.lastEventID = newEvents[len(newEvents)-1][0] #update last event ID
            self.eventsList.extend(newEvents)
            
    """
    Return the last 5 events
    """
    def getLatestEvents(self):
        curLatestEvents = self.pgDB.executeQuery("SELECT \"eventID\",\"routerIP\",\"type\",\"createTS\" FROM event ORDER BY \"eventID\" DESC LIMIT 5")
        return curLatestEvents.fetchall()


