import os
from flask import Blueprint, g, current_app, render_template, redirect, url_for, session, request, jsonify

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



@admin.route('/adminauth', methods = ['POST'])
def adminauth():

    adminpw = request.get_json().get('adminpw')
    if adminpw == os.environ.get("ADMIN_FUNCTION_PASSWORD"):
        session['AUTHORIZED_FOR_ADMIN_FUNCTIONS'] = True
    
    return jsonify(message=str(session.get("AUTHORIZED_FOR_ADMIN_FUNCTIONS")).lower())
