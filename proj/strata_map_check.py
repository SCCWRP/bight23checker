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

    if any([os.path.exists(grab_json_path), os.path.exists(trawl_json_path)]):
        return render_template(f'map_template.html', submissionid=session['submissionid'])
    else:
        return "Map not generated because there are no spatial errors. Ignore this tab"

    
getgeojson = Blueprint('getgeojson', __name__)
@getgeojson.route('/getgeojson', methods = ['GET','POST'], strict_slashes=False)
def send_geojson():

    arcgis_api_key = os.environ.get('ARCGIS_API_KEY')
    
    path_to_grab_json = os.path.join(os.getcwd(), "files", str(session.get('submissionid')), "bad_grab.json")
    path_to_trawl_json = os.path.join(os.getcwd(), "files", str(session.get('submissionid')), "bad_trawl.json")
    path_to_trawl_strata_json = os.path.join(os.getcwd(), "files", str(session.get('submissionid')), "bad_trawl_bight_regions.json")
    path_to_grab_strata_json = os.path.join(os.getcwd(), "files", str(session.get('submissionid')), "bad_grab_bight_regions.json")
    path_to_target_json = os.path.join(os.getcwd(), "files", str(session.get('submissionid')), "target_stations.json")
    
    if os.path.exists(path_to_grab_json):
        with open(path_to_grab_json, 'r') as f:
            points = json.load(f)
    else: 
        points = "None"

    if os.path.exists(path_to_trawl_json):
        with open(path_to_trawl_json, 'r') as f:
            polylines = json.load(f)
    else:
        polylines = "None"
    
    if os.path.exists(path_to_trawl_strata_json):
        with open(path_to_trawl_strata_json, 'r') as f:
            trawl_polygons = json.load(f)
    else:
        trawl_polygons = "None"
    
    if os.path.exists(path_to_grab_strata_json):
        with open(path_to_grab_strata_json, 'r') as f:
            grab_polygons = json.load(f)
    else:
        grab_polygons = "None"


    if os.path.exists(path_to_target_json):
        with open(path_to_target_json, 'r') as f:
            targets = json.load(f)
    else:
        targets = "None"

    return jsonify(targets = targets, points=points, polylines=polylines, trawl_polygons=trawl_polygons, grab_polygons=grab_polygons, arcgis_api_key=arcgis_api_key, strata_layer_id = os.environ.get('BIGHT18_STRATA_LAYER_ID'))