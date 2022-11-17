from flask import render_template, request, jsonify, current_app, Blueprint, session, g, send_from_directory
from werkzeug.utils import secure_filename
from gc import collect
import os
import pandas as pd
import json

map_check = Blueprint('map_check', __name__)
@map_check.route('/map', methods=['GET'], strict_slashes=False)
def getmap():
    
    submissionid = session.get('submissionid')

    grab_json_path = os.path.join(os.getcwd(), "files", str(submissionid), "bad_grab.json")
    trawl_json_path = os.path.join(os.getcwd(), "files", str(submissionid), "bad_trawl.json")
    print(grab_json_path)
    print(os.path.exists(grab_json_path))
    print(os.path.exists(trawl_json_path))
    
    if any([os.path.exists(grab_json_path), os.path.exists(trawl_json_path)]):
        return render_template(f'map_template.html', submissionid=session['submissionid'])
    else:
        return "Map not generated because there are no spatial errors. Ignore this tab"

    
getgeojson = Blueprint('getgeojson', __name__)
@getgeojson.route('/getgeojson', methods = ['GET','POST'], strict_slashes=False)
def send_geojson():

    path_to_grab_json = os.path.join(os.getcwd(), "files", str(session.get('submissionid')), "bad_grab.json")
    path_to_trawl_json = os.path.join(os.getcwd(), "files", str(session.get('submissionid')), "bad_trawl.json")
    
    with open(path_to_grab_json, 'r') as f:
        points = json.load(f)

    with open(path_to_trawl_json, 'r') as f:
        polylines = json.load(f)
    
    print("Printing points and polylines")
    print(points)
    print(polylines)

    # points = [
    #     {"type": "point", "longitude": -118.80657463861,"latitude": 34.0005930608889}, 
    #     {"type": "point", "longitude": -118.80657463861,"latitude": 34.0005930608889}
    # ]

    # polylines = [
    #     {
    #         "type": "polyline", 
    #         "paths": [
    #             [-118.821527826096, 34.0139576938577], 
    #             [-118.814893761649, 34.0080602407843], 
    #             [-118.808878330345, 34.0016642996246]  
    #         ]
    #     }
    # ]
    arcgis_api_key = os.environ.get('ARCGIS_API_KEY')
    return jsonify(points=points, polylines=polylines, arcgis_api_key=arcgis_api_key)