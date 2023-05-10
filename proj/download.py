import os, time
from flask import send_file, Blueprint, jsonify, request, g, current_app, render_template, send_from_directory
import pandas as pd
from pandas import read_sql, DataFrame
import re

download = Blueprint('download', __name__)
@download.route('/download/<submissionid>/<filename>', methods = ['GET','POST'])
def submission_file(submissionid, filename):
    return send_file( os.path.join(os.getcwd(), "files", submissionid, filename), as_attachment = True, download_name = filename ) \
        if os.path.exists(os.path.join(os.getcwd(), "files", submissionid, filename)) \
        else jsonify(message = "file not found")

@download.route('/export', methods = ['GET','POST'])
def template_file():
    filename = request.args.get('filename')
    tablename = request.args.get('tablename')
    agency = request.args.get('agency')
    if request.args.get("agency"):
        agencycode = request.args.get("agency")
        if agencycode == "ABC":
            agency = "Aquatic Bioassay and Consulting Laboratories"
        elif agencycode == "ANCHOR":
            agency = "Anchor QEA"
        elif agencycode == "AMEC":
            agency = "AMEC, Foster, & Wheeler / WOOD"
        elif agencycode == "CSD":
            agency = "City of San Diego"
        elif agencycode == "LACSD":
            agency = "Los Angeles County Sanitation Districts"
        elif agencycode == "MBC":
            agency = "Marine Biological Consulting"
        elif agencycode == "OCSD":
            agency = "Orange County Sanitation Districts"
        elif agencycode == "CLAEMD":
            agency = "City of Los Angeles Environmental Monitoring Division"
        elif agencycode == "CLAWPD":
            agency = "City of Los Angeles Watershed Protection Division"
        elif agencycode == "SCCWRP":
            agency = "Southern California Coastal Water Research Project"
        elif agencycode == "SPAWAR":
            agency = "Space and Naval Warfare Systems Command"
        elif agencycode == "WESTON":
            agency = "Weston Solutions"
        print(f'agency: {agency}')
        # variables use timestamp at end of the export file IF storing and downloading export data file gives an issue
        #gettime = int(time.time())
        #timestamp = str(gettime)

        # add another folder within /export folder for returning data to user (either in browser or via excel file)
        # probably better to not store all these queries in an excel file for storage purposes - use timestamp if this is an issue
        # name after agency and table for now
        export_name = f'{agencycode}-export.xlsx'
        export_file = os.path.join(os.getcwd(), "export", "data_query", export_name)
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
	                                occupationlon as occupationlongitude,
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

        # call to database to get trawl data
        trawl_df = pd.read_sql(f""" SELECT 
                                        trawlstationid as stationid,
                                        date(trawloverdate) as sampledate,
                                        trawlsamplingorganization as samplingorganization,
                                        trawlgear as gear,
                                        trawlnumber,
                                        trawldatum as datum, 
                                        (trawloverdate-interval'7 hours')::time as overtime, 
                                        trawlovery as overlatitude, 
                                        trawloverx as overlongitude,
                                        (trawlstartdate-interval'7 hours')::time as starttime, 
                                        trawlstarty as startlatitude, 
                                        trawlstartx as startlongitude,
                                        trawlstartdepth as startdepth, 
                                        trawldepthunits as depthunits, 
                                        trawlwireout as wireout,
                                        (trawlenddate-interval'7 hours')::time as endtime, 
                                        trawlendy as endlatitude, 
                                        trawlendx as endlongitude,
                                        trawlenddepth as enddepth, 
                                        (trawldeckdate-interval'7 hours')::time as decktime, 
                                        trawldecky as decklatitude, 
                                        trawldeckx as decklongitude, 
                                        trawlfail, 
                                        ptsensor, 
                                        ptsensormanufacturer, 
                                        ptsensorserialnumber,
                                        netonbottomtemp as onbottomtemp, 
                                        netonbottomtime as onbottomtime, 
                                        trawlcomments as comments
                                    FROM mobile_trawl
                                    WHERE
                                        trawlsamplingorganization = '{agency}';
                                """, eng)
        print("trawl_df:")
        print(trawl_df)
        print(type(trawl_df))

        # call to database to get grab data
        grab_df = pd.read_sql(f"""SELECT
                                    grabstationid as stationid,
                                    date(grabdate) as sampledate, 
                                    to_char((grabdate-interval'7 hours'), 'HH24:MI:SS') as sampletime,
                                    grabnumber as grabeventnumber,
                                    grabsamplingorganization as samplingorganization,
                                    grabgear as gear, 
                                    grabx as latitude,
                                    graby as longitude, 
                                    grabdatum as datum, 
                                    grabstationwaterdepth as stationwaterdepth,
                                    grabstationwaterdepthunits as stationwaterdepthunits, 
                                    grabpenetration as penetration, 
                                    grabpenetrationunits as penetrationunits, 
                                    grabsedimentcomposition as composition, 
                                    grabsedimentcolor as color, 
                                    grabsedimentodor as odor, 
                                    grabshellhash as shellhash, 
                                    benthicinfauna, 
                                    sedimentchemistry, 
                                    grainsize, 
                                    toxicity, 
                                    grabfail, 
                                    debris as debrisdetected, 
                                    grabcomments as comments 
                                FROM mobile_grab
                                WHERE grabsamplingorganization = '{agency}';
                                """, eng)
        print("grab_df: ")
        print(grab_df)
        print(type(grab_df))


    if filename is not None:
        return send_file( os.path.join(os.getcwd(), "export", "data_query", filename), as_attachment = True, download_name = filename ) \
            if os.path.exists(os.path.join(os.getcwd(), "export", "data_query", filename)) \
            else jsonify(message = "file not found")
    
    with export_writer:
        occupation_df.to_excel(export_writer, sheet_name = "occupation", index = False)
        trawl_df.to_excel(export_writer, sheet_name = "trawl", index = False)
        grab_df.to_excel(export_writer, sheet_name = "grab", index = False)
    

    return render_template('export.html', export_name=export_name, agency=agency)


# idea is to serve export.html above, then have this route serve the exported file
@download.route('/export/data_query/<export_name>', methods = ['GET','POST'])
def data_query(export_name):
    return send_from_directory(os.path.join(os.getcwd(), "export", "data_query"), export_name, as_attachment=True)

@download.route('/download/<table>', methods = ['GET','POST'])
def get_table(table):
    print(table)

    pattern = "[^\w\s]"

    if bool(re.search(pattern, table)):
        return "special characters detected in the input"

    qry = f"SELECT * FROM {table};"
        
    print(qry)
    datapath = os.path.join(os.getcwd(), 'export', f'{table}.csv')
    data = pd.read_sql(qry, g.eng)
    data.drop(columns=[x for x in data.columns if x in current_app.system_fields]).to_csv(datapath, index=False)

    return send_file(datapath, download_name = f'{table}.csv', as_attachment = True)

# def template_file():
#     filename = request.args.get('filename')
#     tablename = request.args.get('tablename')

#     if filename is not None:
#         return send_file( os.path.join(os.getcwd(), "export", "data_query", filename), as_attachment = True, download_name = filename ) \
#             if os.path.exists(os.path.join(os.getcwd(), "export", "data_query", filename)) \
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

#         return send_file( datapath, as_attachment = True, download_name = f'{tablename}.csv' )

#     else:
#         return jsonify(message = "neither a filename nor a database tablename were provided")
