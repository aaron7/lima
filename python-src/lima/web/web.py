'''
Created on 1 Feb 2013

@author: aaron

IMPORTANT: Do not run using the normal python interface.
#1: Use the python virtual enviroment by running: python-env/bin/activate
#2: Run using gunicorn by running: gunicorn --debug --worker-class=gevent -t 99999 web:app
'''

import flask
import redis
import random
import json

from database import PostgresDB, HBaseDB
from events import EventsHandler
from router import RouterHandler
from flask import request

#start up flask and redis
app = flask.Flask(__name__)
red = redis.StrictRedis()

#make connections to databases
postgresDB = PostgresDB()
hbaseDB = HBaseDB()
eventsHandler = EventsHandler(postgresDB,red)
routerHandler = RouterHandler(postgresDB,red)

'''
Other
'''


'''
End of other
'''

'''
Web routes
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

'''
End of web routes
'''


"""subscribe to a list of channels and listen to these channels
"""
def event_stream(channels):
    pubsub = red.pubsub()
    pubsub.subscribe(channels)
    for message in pubsub.listen():
        print message
        yield 'data: %s\n\n' % message['data']

"""test function
"""
@app.route("/updateEvents")
def updateEvents():
    eventsHandler.checkNewEvents();
    #red.publish("events", eventsHandler.getEventsList())
    #red.publish("events", eventsHandler.getEventsJSON())
    # num = random.randint(10000000,99999999) #make a random number
    # tablelist = ', '.join(hbaseDB.getTables()) #get the tables from HBase
    # data = {"count":str(num), "tables":tablelist} #make a json like data structure
    # red.publish('events', json.dumps(data)) #push into the events channel through a json dump
    return ("Done")

@app.route("/updateRouters")
def updateRouters():
    routerHandler.checkRouterUpdates()
    return ("Done")

@app.route("/java/<action>")
def java(action):
    if action == "newJob":
        #routerHandler.addJob(request.args['ip'], request.args['numOfJobs'])
        print request.args['ip'] + ":" + request.args['numOfJobs']
    elif action == "updateJob":
        #routerHandler.updateJob(routerHandler.addJob(request.args['ip'], request.args['inc']))
        print request.args['ip'] + ":" + request.args['inc']
    return ("Done")
    
    
"""make a stream to a list of channels /stream?channels=ch1,ch2,ch4
"""
@app.route('/stream')
def stream():
    channels = request.args['channels'].split(',') #get the list of channels
    return flask.Response(event_stream(channels), mimetype="text/event-stream")

"""get rest query to get static or initial data from the server
"""
@app.route('/get')
def get():
    if request.args['id'] == 'events':
        return json.dumps(eventsHandler.getEventsList());
    elif request.args['id'] == 'routers':
        return json.dumps(routerHandler.getRoutersList());
    else:
        return "Error: did not understand arguments"

    #elif request.args['id'] == 'allrouters':
    #  return routerHandler.getRoutersJSON();
    

if __name__ == "__main__":
    app.run(port=7777, threaded=True, debug=True)
    """
    flask.url_for("static", filename='main.css')
    flask.url_for("static", filename='bootstrap.css')
    flask.url_for("static", filename='bootstrap-responsive.css')
    flask.url_for("static", filename='jquery.min.js')
    flask.url_for("static", filename='bootstrap.min.js')
    flask.url_for("static", filename='jquery.dataTables.min.js')
    flask.url_for("static", filename='jquery.dataTables.css')
    flask.url_for("static", filename='jquery.dataTables_themeroller.css')
    """
    
