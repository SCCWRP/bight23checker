import os, time
from flask import send_file, Blueprint, jsonify, request, g, current_app, render_template, send_from_directory
import pandas as pd
from pandas import read_sql, DataFrame

report_bp = Blueprint('report', __name__)

@report_bp.route('/report', methods = ['GET','POST'])
def report():
    valid_datatypes = ['field', 'chemistry', 'infauna', 'toxicity']
    datatype = request.args.get('datatype')
    if datatype is None:
        return "No datatype specified"
    if datatype in valid_datatypes:
        report_df = pd.read_sql(f'select * from vw_{datatype}_completeness_report', g.eng)
        report_df.set_index(['submissionstatus', 'lab', 'parameter'], inplace = True)
    else:
        report_df = pd.DataFrame(columns = ['submissionstatus', 'lab', 'parameter', 'stations'])
        report_df.set_index(['submissionstatus', 'lab'], inplace = True)

    return render_template(
        'report.html',  
        tables=[report_df.to_html(classes=['w3-table','w3-bordered'], header="true", justify = 'left', sparsify = True)], 
        report_title = f'{datatype.capitalize()} Completeness Report'
    )
