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
        print("No datatype specified")
        return render_template(
            'report.html',
            datatype=datatype
        )
    if datatype in valid_datatypes:
        report_df = pd.read_sql(f'select * from vw_{datatype}_completeness_report', g.eng)
        report_df.set_index(['submissionstatus', 'lab', 'parameter'], inplace = True)
    else:
        report_df = pd.DataFrame(columns = ['submissionstatus', 'lab', 'parameter', 'stations'])
        report_df.set_index(['submissionstatus', 'lab'], inplace = True)

    return render_template(
        'report.html',
        datatype=datatype,
        tables=[report_df.to_html(classes=['w3-table','w3-bordered'], header="true", justify = 'left', sparsify = True)], 
        report_title = f'{datatype.capitalize()} Completeness Report'
    )


# We need to put the warnings report code here
# Logic is to have a page that displays datatypes and allows them to select a datatype
# after selecting a datatype, they should be able to select a table that is associated with that datatype
# All this information is in the proj/config folder
#
# after selecting a table, it should display all warnings from that table 
# (each table has a column called warnings, it is a varchar field and the string of warnings is formatted a certain way)
# example: 
#  columnname - errormessage; columnname - errormessage2; columnname - errormessage3
# Would it have been better if we would have made it a json field? probably, but there must be some reason why we did it like this
#
# so when they select the table, we need to get all the warnings associated with that table, 
#  selecting from that table where warnings IS NOT NULL
# Then we have to pick apart the warnings column text, gather all unique warnings and display to the user
# We need them to have the ability to select warnings, and then download all records from that table which have that warning

# a suggestion might be to do this how nick did the above, where he requires a query string arg, 
# except we should put logic such that if no datatype arg is provided, we return a template that has links with the datatype query string arg

# example 
#  <a href="/warnings-report?datatype=chemistry">Chemistry</a>
#  <a href="/warnings-report?datatype=toxicity">Toxicity</a>
# etc

@report_bp.route('/warnings-report')
def warnings_report():
    eng = g.eng

    datatype = request.args.get('datatype')

    if datatype is not None:
        print('tables')
        tables = current_app.datasets.get(datatype).get('tables')
        print(tables)

    # tmp = pdread_sql('SELECT * FROM {table} WHERE warnings IS NOT NULL', eng)
    # warnings_array = tmp.warnings.apply(lambda x: [s.split(' - ', 1)[-1] for s in x.split(';')]).values
    # [item for sublist in warnings_array for item in sublist]
    # unique_warnings = pd.Series([item for sublist in warnings_array for item in sublist]).unique()
    
    # pd.read_sql(f"SELECT * FROM tbl_toxwq WHERE warnings LIKE '%%{test}%%'", eng)
    
    print('hello world')
    return 'hello world'

