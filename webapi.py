import flask
from flask import request

import MonCassa
app = flask.Flask(__name__)

@app.route("/write", methods=['POST'])
def write():
    metric = request.form['metric']
    timestamp = request.form['timestamp']
    value = request.form['value']
    MonCassa.write(metric, long(timestamp), float(value))
    return flask.jsonify(success=True)

#input metric , starttime, endtime, return data points
@app.route("/read", methods=['POST'])
def read():
    metric = request.form['metric']
    starttime = request.form['starttime']
    endtime = request.form['endtime']
    result_points = MonCassa.read(metric, long(starttime), long(endtime))
    return flask.jsonify(result_points)

if __name__ == "__main__":
    app.run()
