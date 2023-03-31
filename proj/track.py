import os
from flask import Blueprint, g, current_app, render_template, redirect, url_for, session, request, jsonify

track = Blueprint('track', __name__)

@track.route('/track')
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
    authorized = session.get("AUTHORIZED_FOR_TRACKER")
    
    # session is a reserved word in flask - renaming to something different
    return render_template('track.html', session_json=session_json, authorized=authorized)



@track.route('/trackauth', methods = ['POST'])
def trackauth():
    trackpw = request.form.get("trackpw")
    session['AUTHORIZED_FOR_TRACKER'] = trackpw == os.environ.get("TRACKER_PASSWORD")
    
    return jsonify(message=str(session.get("AUTHORIZED_FOR_TRACKER")).lower())
