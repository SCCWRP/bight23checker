import os, time
from flask import send_file, Blueprint, jsonify, request, g, current_app
import pandas as pd
from pandas import read_sql, DataFrame

download = Blueprint('download', __name__)
@download.route('/download/<submissionid>/<filename>', methods = ['GET','POST'])
def submission_file(submissionid, filename):
    return send_file( os.path.join(os.getcwd(), "files", submissionid, filename), as_attachment = True, attachment_filename = filename ) \
        if os.path.exists(os.path.join(os.getcwd(), "files", submissionid, filename)) \
        else jsonify(message = "file not found")

@download.route('/export', methods = ['GET','POST'])
def template_file():
    #filename = request.args.get('filename')
    #tablename = request.args.get('tablename')
    agency = request.args.get('agency')
    if request.args.get("agency"):
        agencycode = request.args.get("agency")
        if agencycode == "ABC":
            agency = "Aquatic Bioassay and Consulting Laboratories"
        print(f'agency: {agency}')
        # variables use timestamp at end of the export file IF storing and downloading export data file gives an issue
        #gettime = int(time.time())
        #timestamp = str(gettime)

        # add another folder within /export folder for returning data to user (either in browser or via excel file)
        # probably better to not store all these queries in an excel file for storage purposes - use timestamp if this is an issue
        # name after agency and table for now
        export_file = f'/var/www/checker/export/data_query/{agencycode}-export.xlsx'
        export_link = f'http://checker.sccwrp.org/checker/export/data_query/{agencycode}-export.xlsx'
        export_writer = pd.ExcelWriter(export_file, engine='xlsxwriter')
        eng = g.eng

        #call database to get occupation data
        occupation_df = pd.read_sql(f"""SELECT 
                                    stationid, 
                                    date(occupationdate) as occupationdate,
                                    to_char((occupationdate-interval'7 hours'), 'HH24:MI:SS') as occupationtime,
                                    timezone as occupationtimezone,
                                    samplingorganization,
	                                collectiontype,
	                                vessel,
	                                navigationtype as navtype,
	                                salinity,
	                                salinityunits,
	                                weather,
	                                windspeed,
	                                windspeedunits,
	                                winddirection,
	                                swellheight,
                                    swellheightunits,
	                                swellperiod, 
                                    swelldirection,
	                                seastate, 
	                                stationfail,
	                                abandoned,
	                                stationdepth as occupationdepth,
                                    stationdepthunits as occupationdepthunits,
	                                occupationlat as occupationlatitude, 
	                                occupationlon as occupationolongitude,
	                                datum as occupationdatum,
	                                stationcomments as comments
                                FROM mobile_occupation_trawl
                                WHERE samplingorganization = '{agency}'
                                UNION
                                SELECT
                                    stationid,
                                    date(occupationdate) as occupationdate,
                                    to_char((occupationdate-interval'7 hours'),'HH24:MI:SS') as occupationtime,
                                    timezone as occupationtimezone,
                                    samplingorganization,
                                    collectiontype,
                                    vessel,
                                    navigationtype as navtype,
                                    salinity,
                                    salinityunits,
                                    weather,
                                    windspeed,
                                    windspeedunits,
                                    winddirection,
                                    swellheight,
                                    swellheightunits,
                                    swellperiod,
                                    swelldirection,
                                    seastate,
                                    stationfail,
                                    abandoned,
                                    stationdepth as occupationdepth,
                                    stationdepthunits as occupationdepthunits,
                                    occupationlat as occupationlatitude,
                                    occupationlon as occupationlongitude,
                                    datum as occupationdatum,
                                    stationcomments as comments
                                FROM
                                    mobile_occupation_grab
                                WHERE samplingorganization = '{agency}';
                            """, eng)
    print("working checkpoint")
    print("occupation_df: ")
    print(occupation_df)


    if filename is not None:
        return send_file( os.path.join(os.getcwd(), "export", "data_templates", filename), as_attachment = True, attachment_filename = filename ) \
            if os.path.exists(os.path.join(os.getcwd(), "export", "data_templates", filename)) \
            else jsonify(message = "file not found")
    
    elif tablename is not None:
        eng = g.eng
        valid_tables = read_sql("SELECT table_name FROM information_schema.tables WHERE table_name LIKE 'tbl%%';", g.eng).values
        
        if tablename not in valid_tables:
            return "invalid table name provided in query string argument"
        
        data = read_sql(f"SELECT * FROM {tablename};", eng)
        data.drop( set(data.columns).intersection(set(current_app.system_fields)), axis = 1, inplace = True )

        datapath = os.path.join(os.getcwd(), "export", "data", f'{tablename}.csv')

        data.to_csv(datapath, index = False)

        return send_file( datapath, as_attachment = True, attachment_filename = f'{tablename}.csv' )

    else:
        return jsonify(message = "neither a filename nor a database tablename were provided")

# def template_file():
#     filename = request.args.get('filename')
#     tablename = request.args.get('tablename')

#     if filename is not None:
#         return send_file( os.path.join(os.getcwd(), "export", "data_templates", filename), as_attachment = True, attachment_filename = filename ) \
#             if os.path.exists(os.path.join(os.getcwd(), "export", "data_templates", filename)) \
#             else jsonify(message = "file not found")
    
#     elif tablename is not None:
#         eng = g.eng
#         valid_tables = read_sql("SELECT table_name FROM information_schema.tables WHERE table_name LIKE 'tbl%%';", g.eng).values
        
#         if tablename not in valid_tables:
#             return "invalid table name provided in query string argument"
        
#         data = read_sql(f"SELECT * FROM {tablename};", eng)
#         data.drop( set(data.columns).intersection(set(current_app.system_fields)), axis = 1, inplace = True )

#         datapath = os.path.join(os.getcwd(), "export", "data", f'{tablename}.csv')

#         data.to_csv(datapath, index = False)

#         return send_file( datapath, as_attachment = True, attachment_filename = f'{tablename}.csv' )

#     else:
#         return jsonify(message = "neither a filename nor a database tablename were provided")
