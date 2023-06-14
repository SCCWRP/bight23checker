import os
import pandas as pd
from bs4 import BeautifulSoup
from io import BytesIO
from flask import Blueprint, g, current_app, render_template, redirect, url_for, session, request, jsonify, send_file

from .utils.db import metadata_summary

admin = Blueprint('admin', __name__)

@admin.route('/track')
def tracking():
    print("start track")
    sql_session =   '''
                    SELECT LOGIN_EMAIL,
                        LOGIN_AGENCY,
                        SUBMISSIONID,
                        DATATYPE,
                        SUBMIT,
                        CREATED_DATE,
                        ORIGINAL_FILENAME
                    FROM SUBMISSION_TRACKING_TABLE
                    WHERE SUBMISSIONID IS NOT NULL
                        AND ORIGINAL_FILENAME IS NOT NULL
                    ORDER BY CREATED_DATE DESC
                    '''
    session_results = g.eng.execute(sql_session)
    session_json = [dict(r) for r in session_results]
    authorized = session.get("AUTHORIZED_FOR_ADMIN_FUNCTIONS")
    
    # session is a reserved word in flask - renaming to something different
    return render_template('track.html', session_json=session_json, authorized=authorized)


@admin.route('/schema')
def schema():
    print("entering schema")

    # This is kind of obsolete - orgiinally i was going to have this only available to scientists
    # We will keep this because later we will have different levels of access and privileges
    authorized = session.get("AUTHORIZED_FOR_ADMIN_FUNCTIONS")

    print("start schema information lookup routine")
    eng = g.eng

    # Query string arg to get the specific datatype
    datatype = request.args.get("datatype")

    # Query string arg option to download
    download = str(request.args.get("download")).strip().lower() == 'true'
    
    # If a specific datatype is selected then display the schema for it
    if datatype is not None:
        if datatype not in current_app.datasets.keys():
            return f"Datatype {datatype} not found"

        # dictionary to return
        return_object = {}
        
        tables = current_app.datasets.get(datatype).get("tables")
        for tbl in tables:
            df = metadata_summary(tbl, eng)
            
            df['lookuplist_table_name'] = df['lookuplist_table_name'].apply(
                lambda x: f"""<a target=_blank href=/{current_app.script_root}/scraper?action=help&layer={x}>{x}</a>""" if pd.notnull(x) else ''
            )

            # drop "table_name" column
            df.drop('tablename', axis = 'columns', inplace = True)

            # drop system fields
            df.drop(df[df.column_name.isin(current_app.system_fields)].index, axis = 'rows', inplace = True)

            df.fillna('', inplace = True)

            return_object[tbl] = df.to_dict('records')
        
        if download:
            excel_blob = BytesIO()

            with pd.ExcelWriter(excel_blob) as writer:
                for key in return_object.keys():
                    df_to_download = pd.DataFrame.from_dict(return_object[key])
                    df_to_download['lookuplist_table_name'] = df_to_download['lookuplist_table_name'].apply(
                        lambda x: "https://{}/{}/scraper?action=help&layer={}".format(
                            request.host,
                            current_app.config.get('APP_SCRIPT_ROOT'),
                            BeautifulSoup(x, 'html.parser').text.strip()
                        ) if BeautifulSoup(x, 'html.parser').text.strip() != '' else ''
                    )
                    df_to_download.to_excel(writer, sheet_name=key, index=False)

            excel_blob.seek(0)

            # if the query string said "download=true"
            return send_file(
                excel_blob, 
                download_name = f'{datatype}_schema.xlsx', 
                as_attachment = True, 
                mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
            )

        # Return the datatype query string arg - the template will need access to that
        return render_template('schema.html', metadata=return_object, datatype=datatype, authorized=authorized)
        
    # only executes if "datatypes" not given
    datatypes_list = current_app.datasets.keys()
    return render_template('schema.html', datatypes_list=datatypes_list, authorized=authorized)




@admin.route('/adminauth', methods = ['POST'])
def adminauth():

    adminpw = request.get_json().get('adminpw')
    if adminpw == os.environ.get("ADMIN_FUNCTION_PASSWORD"):
        session['AUTHORIZED_FOR_ADMIN_FUNCTIONS'] = True
    
    return jsonify(message=str(session.get("AUTHORIZED_FOR_ADMIN_FUNCTIONS")).lower())
