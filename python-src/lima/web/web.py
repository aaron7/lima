'''
Created on 1 Feb 2013

@author: aaron

IMPORTANT: Do not run using normal the normal python interface.
#1: Use the python virtual enviroment by running: python-env/bin/activate
#2: Run using gunicorn by running: gunicorn --debug --worker-class=gevent -t 99999 web:app
'''

import database #happyBase
import random
import flask
import redis
import json

#start up flask and redis
app = flask.Flask(__name__)
red = redis.StrictRedis()

@app.route("/")
def dashboard():
    return flask.render_template('dashboard.html')

def event_stream():
    pubsub = red.pubsub()
    pubsub.subscribe('events')
    pushtoclients() #initial push
    for message in pubsub.listen():
        print message
        yield 'data: %s\n\n' % message['data']

@app.route("/push")
def pushtoclients():
    num = random.randint(10000000,99999999) #make a random number
    tablelist = ', '.join(database.getTables()) #get the tables from HBase
    data = {"count":str(num), "tables":tablelist} #make a json like data structure
    red.publish('events', json.dumps(data)) #push into the events channel through a json dump
    return ("Done")

@app.route('/stream')
def stream():
    return flask.Response(event_stream(), mimetype="text/event-stream")

if __name__ == "__main__":
    app.run(port=7777)
    flask.url_for("static", filename='main.css')
    flask.url_for("static", filename='bootstrap.css')
    flask.url_for("static", filename='bootstrap-responsive.css')
    flask.url_for("static", filename='jquery.min.js')
    flask.url_for("static", filename='bootstrap.min.js')
    
