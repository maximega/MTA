import flask
import requests
import json
import sys
import os
from flask import Flask, request, render_template, redirect, url_for, session, json

root_dir = os.path.join(os.getcwd(), '..')
sys.path.append(root_dir)

from optimize_data.kmeans.kmeans_opt import execute

app = Flask(__name__)

@app.route('/result', methods=['GET'])
def result():
    zones = request.args['zones']
    percent_max = request.args['max']
    percent_min = request.args['min']
    factor = request.args['factor']
    app = True
    if zones == '':
        zones = 5
    if percent_max == '':
        percent_max = 20
    if percent_min == '':
        percent_min = 1
    if factor == '':
        factor = 1.4
    data = execute(int(zones), float(percent_max), float(percent_min), float(factor), app)

    for x in data:
        x['_id'] = ""

    res = app.response_class(
        response=json.dumps(data),
        status=200,
        mimetype='application/json'
    )

    return res

if __name__ == "__main__":
    # this is invoked when in the shell  you run
    # $ python app.py
    app.run(port=8080, debug=True)
