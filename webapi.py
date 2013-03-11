import flask
from flask import request
import json
import logging

import MonCassa
app = flask.Flask(__name__)

@app.route("/write", methods=['POST'])
def write():
    metric = request.form['metric']
    timestamp = request.form['timestamp']
    value = request.form['value']
    tags = request.form['tags']
    json_tag = json.loads(tags)
    try:
        MonCassa.write(metric, long(timestamp), float(value), json_tag)
    except Exception as e:
        logging.exception('Some exception occured, values that caused it\n'
                          'metric: %s\ntags: %s\ntimestamp: %s\nvalue: %s' %
                          (metric, tags, timestamp, value))
    return flask.jsonify(success=True)

#input metric , starttime, endtime, return data points
@app.route("/read", methods=['POST'])
def read():
    metric = request.form['metric']
    starttime = request.form['starttime']
    endtime = request.form['endtime']
    tags = request.form['tags']
    json_tag = json.loads(tags)
    result_points = MonCassa.read(metric, long(starttime), long(endtime), json_tag)
    if result_points:
        return flask.jsonify(success=True, results=result_points)
    else:
        return flask.jsonify(success=True, results={})
#    return flask.jsonify(result_points)

if __name__ == "__main__":
    app.run(debug=True)
