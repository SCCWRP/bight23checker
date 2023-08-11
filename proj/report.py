import os, time
from flask import send_file, Blueprint, jsonify, request, g, current_app, render_template, send_from_directory, make_response
import pandas as pd
from pandas import read_sql, DataFrame
import json

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
        report_df = pd.read_sql(f'SELECT * FROM vw_tac_{datatype}_completion_status', g.eng)
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

@report_bp.route('/warnings-report',  methods = ['GET','POST'])
def warnings_report():
    datatype = request.args.get('datatype')
    print(datatype)
    table = request.args.get('table')
    print(table)
    if datatype is None and table is None:
        json_file = open('proj/config/config.json') 
        data = json.load(json_file)
        dataset_options = data["DATASETS"].keys()
        print("Missing fields")
        return render_template(
            'warnings-report.html',
            dataset_options = dataset_options
            )
    elif table is None :
        json_file = open('proj/config/config.json') 
        data = json.load(json_file)
        dataset_options = data["DATASETS"].keys()
        table_options =data['DATASETS'][datatype]['tables']
        print("Missing fields")
        return render_template(
            'warnings-report.html',
            dataset_options = dataset_options,
            table_options = table_options
            )
    else: 
        eng = g.eng
        sql_query = f"SELECT * FROM {table} WHERE warnings IS NOT NULL"
        tmp = pd.read_sql(sql_query, eng)
        warnings_array = tmp.warnings.apply(lambda x: [s.split(' - ', 1)[-1] for s in x.split(';')]).values
        unique_warnings = pd.Series([item for sublist in warnings_array for item in sublist]).unique()
        df = pd.DataFrame(unique_warnings, columns = ["Warnings"])
        print(df)
        # warnings_table = df.to_html(header="true", table_id="table")


        bad_dictionary={}
        i=0
        for row in df.Warnings:    
            bad_data= pd.read_sql(f"SELECT * FROM {table} WHERE warnings LIKE '%%{row}%%'", eng)

            # bad_data.to_excel("downloadable_data.xlsx", index=False)
            # df["downloadable_data"] = downloadable_data
            print(bad_data)


            # bad_dictionary[f"errors{i}"] = bad_data
            # print(bad_dictionary.get(f"errors{i}"))
            #downloadable_data = bad_dictionary.get(f"errors{i}").to_csv(index=False, encoding='utf-8')
            # print(downloadable_data)
            # i=i+1
            # print("helloWorld")
            # Need to write download thingy here 
            # df["Download Data"]= csv_data

            # df.rows= pd.DataFrame(bad_data, columns = ["Bad Data"])
            # print(csv_data)



            
        return render_template(
            'warnings-report.html',
            datatype=datatype,
            table = table,
            warnings_table = [df.to_html(classes='data')], titles=df.columns.values,
            df=df)
        
@report_bp.route('/warnings-report/download/<export_name>', methods = ['GET','POST'])
def download(export_name):
#     # bad_data= pd.read_sql(f"SELECT * FROM {table} WHERE warnings LIKE '%%{row}%%'", eng)

#     # bad_data.to_excel("downloadable_data.xlsx", index=False)
#     # df["downloadable_data"] = downloadable_data  
#     #   w = 1
    return send_from_directory(os.path.join(os.getcwd(), "export", "data_query"), export_name, as_attachment=True)