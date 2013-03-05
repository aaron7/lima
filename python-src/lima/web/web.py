'''
Lima Web UI

IMPORTANT: Do not run using the normal python interface.
Read the README.txt file
'''

import flask
import redis
import json

from database import PostgresDB, HBaseDB
from events import EventsHandler
from router import RouterHandler
from flask import request

#start up flask and redis
app = flask.Flask(__name__)
red = redis.StrictRedis()

#make the connections to the databases and handlers
postgresDB = PostgresDB()
hbaseDB = HBaseDB()
eventsHandler = EventsHandler(postgresDB,red)
routerHandler = RouterHandler(postgresDB,red,hbaseDB)

'''
Web routes

Routes the address to the correct flask template
'''
@app.route("/")
def dashboard():
    return flask.render_template('dashboard.html')
@app.route("/routers")
def routers():
    return flask.render_template('routers.html')
@app.route("/events")
def events():
    return flask.render_template('events.html')
@app.route("/status")
def status():
    return flask.render_template('status.html')

"""
Subscribe to the list of channels given in the argument and listen to them
"""
def event_stream(channels):
    pubsub = red.pubsub()
    pubsub.subscribe(channels)
    for message in pubsub.listen():
        print message
        yield "data: [%s,\"%s\"]\n\n" % (message['data'],message['channel'])

"""
Used for demonstration purposes to update the events and routers from PostgreSQL
"""
@app.route("/updateEvents")
def updateEvents():
    eventsHandler.checkNewEvents();
    return ("Done")
@app.route("/updateRouters")
def updateRouters():
    routerHandler.checkRouterUpdates()
    return ("Done")
@app.route("/purgeCache")
def purgeCache():
    routerHandler.checkRouterUpdates()
    routerHandler.purge()
    eventsHandler.purge()
    return ("Done")

"""
Interface for the Java map reduce jobs and the Event Monitor

NewJob sets up a new map reduce job for a router, defining the number of parts to be completed
UpdateJob increments the job counter and if complete is passed as true then the job is updated
to be finished
"""
@app.route("/java/<action>")
def java(action):
    if action == "newJob":
        routerHandler.addJob(request.args['ip'], request.args['timestamp'], int(request.args['numOfJobs']))
    elif action == "updateJob":
        routerHandler.updateJob(request.args['ip'], request.args['timestamp'], int(request.args['complete']))
    return ("Done")
    
"""
An interface for a stream to a list of channels
e.g. /stream?channels=ch1,ch2,ch4
"""
@app.route('/stream')
def stream():
    channels = request.args['channels'].split(',') #get the list of channels
    return flask.Response(event_stream(channels), mimetype="text/event-stream")

"""
Get API query to poll from the server

events - return all of the events as JSON
routers - return all of the routers as JSON
largeData - return all of the data from the Statistic table in HBase for a particular router
eventData - return all the events associated with the router given
threatData - return the data from the Threat table in HBase for a particular event
latestEvents - return the last 5 events
runningJobs - return details of routers which have a job running
allLargeData - return the complete total of the data from each router in the Statistic table in HBase
"""
@app.route('/get')
def get():
    if request.args['id'] == 'events':
        return json.dumps(eventsHandler.getEventsList())
    elif request.args['id'] == 'routers':
        return json.dumps(routerHandler.getRoutersList())
    elif request.args['id'] == 'largeData':
        return json.dumps(routerHandler.getLargeData(request.args['router']))
    elif request.args['id'] == 'eventData':
        return json.dumps(routerHandler.getEventData(request.args['router']))
    elif request.args['id'] == 'threatData':
        return json.dumps(routerHandler.getThreatData(request.args['timestamp'],request.args['routerIP'],request.args['type'],request.args['startTime'],request.args['endTime']))
    elif request.args['id'] == 'latestEvents':
        return json.dumps(eventsHandler.getLatestEvents())
    elif request.args['id'] == 'runningJobs':
        return json.dumps(routerHandler.getRunningJobs())
    elif request.args['id'] == 'allLargeData':
        return json.dumps(routerHandler.getAllLargeData())
    else:
        return "Error: did not understand arguments"

"""
main function to start the Flask server
Disable debug when in production.
"""
if __name__ == "__main__":
    app.run(port=7777, threaded=True, debug=True)


