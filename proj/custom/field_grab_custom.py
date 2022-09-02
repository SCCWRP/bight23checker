# Dont touch this file! This is intended to be a template for implementing new custom checks

from inspect import currentframe
from flask import current_app, g
from .functions import checkData, haversine_np, check_distance, check_time
import pandas as pd
import re
from shapely.geometry import Point, LineString
import numpy as np



def field_grab(all_dfs):
    
    current_function_name = str(currentframe().f_code.co_name)
    
    # function should be named after the dataset in app.datasets in __init__.py
    assert current_function_name in current_app.datasets.keys(), \
        f"function {current_function_name} not found in current_app.datasets.keys() - naming convention not followed"

    expectedtables = set(current_app.datasets.get(current_function_name).get('tables'))
    assert expectedtables.issubset(set(all_dfs.keys())), \
        f"""In function {current_function_name} - {expectedtables - set(all_dfs.keys())} not found in keys of all_dfs ({','.join(all_dfs.keys())})"""

    # define errors and warnings list
    errs = []
    warnings = []

    eng = g.eng


    # since often times checks are done by merging tables (Paul calls those logic checks)
    # we assign dataframes of all_dfs to variables and go from there
    # This is the convention that was followed in the old checker
    
    # This data type should only have tbl_example
    # example = all_dfs['tbl_example']

    # Alter this args dictionary as you add checks and use it for the checkData function
    # for errors that apply to multiple columns, separate them with commas
    # args = {
    #     "dataframe": example,
    #     "tablename": 'tbl_example',
    #     "badrows": [],
    #     "badcolumn": "",
    #     "error_type": "",
    #     "is_core_error": False,
    #     "error_message": ""
    # }

    # Example of appending an error (same logic applies for a warning)
    # args.update({
    #   "badrows": df[df.temperature != 'asdf'].index.tolist(),
    #   "badcolumn": "temperature",
    #   "error_type" : "Not asdf",
    #   "error_message" : "This is a helpful useful message for the user"
    # })
    # errs = [*errs, checkData(**args)]

    # return {'errors': errs, 'warnings': warnings}

    occupation = all_dfs['tbl_stationoccupation']
    grab = all_dfs['tbl_grabevent']
    occupation['tmp_row'] = occupation.index
    grab['tmp_row'] = grab.index


    occupation_args = {
        "dataframe": occupation,
        "tablename": 'tbl_stationoccupation',
        "badrows": [],
        "badcolumn": "",
        "error_type": "",
        "is_core_error": False,
        "error_message": ""
    }

    grab_args = {
        "dataframe": grab,
        "tablename": 'tbl_grabevent',
        "badrows": [],
        "badcolumn": "",
        "error_type": "",
        "is_core_error": False,
        "error_message": ""
    }

    # ------- LOGIC CHECKS ------- #
    print('# ------- LOGIC CHECKS ------- #')
    
    # Check - Each Grab record must have a corresponding stationoccupation record
    tmp = occupation.merge(
        grab.assign(present = 'yes'), 
        left_on = ['stationid','occupationdate','samplingorganization'], 
        right_on = ['stationid','sampledate','samplingorganization'], 
        how = 'right',
        suffixes = ('_occ','')
    )
    
    badrows = tmp[pd.isnull(tmp.present)].tmp_row.tolist()
    grab_args.update({
      "badrows": badrows,
      "badcolumn": "StationID,SampleDate,SamplingOrganization",
      "error_type" : "Logic Error",
      "error_message" : "Each Grab record must have a corresponding Occupation record. Records are matched on StationID, SampleDate, and SamplingOrganization."
    })
    errs = [*errs, checkData(**grab_args)]
    del tmp
    del badrows


    # Check the time formats on all time columns
    def checkTime(df, col, args, time_format = re.compile(r'([0-9]{1,2}):[0-5][0-9]:[0-5][0-9]'), custom_errmsg = None):
        """default to checking the 24 hour clock time"""
    
        args.update({
            "badrows": df[~df[col.lower()].map(str).str.match(time_format)].tmp_row.tolist(),
            "badcolumn": col,
            "error_type" : "Formatting Error",
            "error_message" : f"The column {col} is not in a valid 24 hour clock format (HH:MM:SS)" if not custom_errmsg else custom_errmsg
        })
        return checkData(**args)
    
    errs = [
        *errs, 
        checkTime(occupation, 'OccupationTime', occupation_args),
        checkTime(grab, 'SampleTime', grab_args)
    ]
    # ------- END LOGIC CHECKS ------- #
    print('# ------- END LOGIC CHECKS ------- #')


    # ------- Occupation Checks ------- #
    print('# ------- Occupation Checks ------- #')
    ## Kristin - StationOccupation/Trawl/Grab check DepthUnits field make sure nobody has entered feet instead of meters
	## (this is an error not a warning). Generic lookup list allows it, but Dario doesnt want users to be able to enter feet. 
    # Depth units should be in meters, not feet
    print("(occupation[['occupationdepthunits','tmp_row']].where(occupation['occupationdepthunits'].isin(['ft','f'])).dropna())")
    print((occupation[['occupationdepthunits','tmp_row']].where(occupation['occupationdepthunits'].isin(['ft','f'])).dropna()))
    print((occupation[['occupationdepthunits','tmp_row']].where(occupation['occupationdepthunits'].isin(['ft','f'])).dropna()).columns)
    badrows = (occupation[['occupationdepthunits','tmp_row']].where(occupation['occupationdepthunits'].isin(['ft','f'])).dropna()).tmp_row.tolist()
    occupation_args.update({
        "badrows": badrows,
        "badcolumn": 'OccupationDepthUnits',
        "error_type" : "Undefined Error",
        "error_message" : "OccupationDepthUnits should be in meters, not feet"
    })
    errs = [*errs, checkData(**occupation_args)]
    
    # Depth units should be in meters, not feet
    badrows = (grab[['stationwaterdepthunits','tmp_row']].where(grab['stationwaterdepthunits'].isin(['ft','f'])).dropna()).tmp_row.tolist()
    grab_args.update({
        "badrows": badrows,
        "badcolumn": 'stationwaterdepthunits',
        "error_type" : "Undefined Error",
        "error_message" : "stationwaterdepthunits should be in meters, not feet"
    })
    errs = [*errs, checkData(**grab_args)]


    # Comment required if the station was abandoned
    badrows = occupation[['abandoned', 'comments','tmp_row']].where(occupation['abandoned'].isin(['Yes'])).dropna(axis = 0, how = 'all').loc[pd.isnull(occupation['comments'])].tmp_row.tolist()
    occupation_args.update({
        "badrows": badrows,
        "badcolumn": 'Comments',
        "error_type" : "Missing Required Data",
        "error_message" : 'A comment is required if the station was abandoned'
    })
    errs = [*errs, checkData(**grab_args)]

    # Comment required for certain stationfail values
    lu_sf = pd.read_sql("select stationfail from lu_stationfails where commentrequired = 'Yes'", eng)
    stationfail_matches = pd.merge(occupation[['stationfail','comments','tmp_row']],lu_sf, on=['stationfail'], how='inner') 
    stationfail_matches['comments'].replace('', pd.NA, inplace=True)

    badrows = stationfail_matches[pd.isnull(stationfail_matches['comments'])].tmp_row.tolist()
    occupation_args.update({
        "badrows": badrows,
        "badcolumn": 'Comments',
        "error_type" : "Missing Required Data",
        "error_message" : "A comment is required for the value you entered in the StationFail field."
    })
    errs = [*errs, checkData(**occupation_args)]


    # Make sure agency was assigned to that station for the corresponding collection type - Grab or Trawl
    # There should only be one sampling organization per submission - this is just a warning
    sampling_organization = occupation.samplingorganization.unique() 
    if len(sampling_organization) > 1:
        occupation_args.update({
            "badrows": occupation[occupation.samplingorganization == occupation.samplingorganization.min()].tmp_row.tolist(),
            "badcolumn": 'SamplingOrganization',
            "error_type" : "Undefined Warning",
            "error_message" : "More than one agency detected"
        })
        warnings = [*warnings, checkData(**occupation_args)]
    else:
        trawlstations = pd.read_sql(f"SELECT DISTINCT stationid FROM field_assignment_table WHERE trawlagency = '{sampling_organization}'", eng).stationid.tolist()
        badrows = occupation[(occupation.collectiontype != 'Grab') & (~occupation.stationid.isin(trawlstations))].tmp_row.tolist()
        occupation_args.update({
            "badrows": badrows,
            "badcolumn": 'StationID,SamplingOrganization',
            "error_type" : "Undefined Warning",
            "error_message" : f"The organization {sampling_organization} was not assigned to trawl at this station"
        })
        warnings = [*warnings, checkData(**occupation_args)]
        
        grabstations = pd.read_sql(f"SELECT DISTINCT stationid FROM field_assignment_table WHERE grabagency = '{sampling_organization}'", eng).stationid.tolist()
        badrows = occupation[(occupation.collectiontype == 'Grab') & (~occupation.stationid.isin(grabstations))].tmp_row.tolist()
        occupation_args.update({
            "badrows": badrows,
            "badcolumn": 'StationID,SamplingOrganization',
            "error_type" : "Undefined Warning",
            "error_message" : f"The organization {sampling_organization} was not assigned to grab at this station"
        })
        warnings = [*warnings, checkData(**occupation_args)]


    # Check StationOccupation/Salinity - if the station is an Estuary or Brackish Estuary then the salinity is required
    estuaries = pd.read_sql("SELECT stationid, stratum FROM field_assignment_table WHERE stratum IN ('Estuaries', 'Brackish Estuaries');", eng)
    # Only run if they submitted data for estuaries
    if len((occupation[(occupation.stationid.isin(estuaries.stationid))]))!=0 :
        # for matching stationids, make sure Estuary and Brackish Estuary salinity has a value
        print('## Make sure Estuary and Brackish Estuary salinity value is non-empty ##')
        strats = pd.merge(occupation[['stationid','salinity', 'tmp_row']],estuaries, how = 'left', on='stationid')
        occupation_args.update({
            "badrows": strats[pd.isnull(strats.salinity)].tmp_row.tolist(),
            "badcolumn": 'Salinity',
            "error_type": 'Undefined Error',
            "error_message": 'Station in Estuary or Brackish Estuary. Salinity is required and user must enter -88 if measurement is missing.'
        })
        errs = [*errs, checkData(**occupation_args)]



    # Jordan - Station Occupation Latitude/Longitude should be no more than 100M from Field Assignment Table Target Latitude/Longitude otherwise warning
    # Merges SO dataframe and FAT dataframe according to StationIDs
    print("Checks on Station Occupation and Field Assignment Table (SOFAT)")
    so = occupation[['stationid','occupationlatitude','occupationlongitude','tmp_row']]
    fat = pd.read_sql("SELECT * FROM field_assignment_table", eng)
    sofat = pd.merge(so, fat, how = 'left', on ='stationid')

    # Raises Error for Unmatched StationIDs & Distances More than 100M from FAT Target
    print("Raises error for unmatched stationids & distances more than 100m from fat target:")
    print(sofat[sofat['targetlatitude'].isnull()])
    occupation_args.update({
        "badrows": sofat[sofat['targetlatitude'].isnull()].tmp_row.tolist(),
        "badcolumn": 'StationID',
        "error_type": 'Undefined Warning',
        "error_message": 'StationOccupation distance to target check - Could not find StationID in field assignment table.'
    })
    errs = [*errs, checkData(**occupation_args)]


    # Calculates distance between SO Lat/Lon and FAT Lat/Lon according to StationIDs
    print("Calculates distance between so lat/lon and fat lat/lon according to stationids:")
    sofat.dropna(inplace=True)
    sofat['targetlatitude'] = sofat['targetlatitude'].apply(lambda x: float(x))
    sofat['targetlongitude'] = sofat['targetlongitude'].apply(lambda x: float(x))
    sofat['dists'] = haversine_np(sofat['occupationlongitude'],sofat['occupationlatitude'],sofat['targetlongitude'],sofat['targetlatitude'])
    print(sofat['dists'])
    # Raises Warning for Distances calculated above > 100M
    print("Raises warning for distances calculated above > 100m:")
    print(sofat.loc[sofat['dists']>100])

    occupation_args.update({
        "badrows": sofat.loc[sofat['dists']>100].tmp_row.tolist(),
        "badcolumn": 'StationID',
        "error_type": 'Undefined Warning',
        "error_message": 'Distance from Occupation Latitude/Longitude in submission to Target Latitude/Longitude in field assignment table is greater than 100 meters.'
    })
    errs = [*errs, checkData(**occupation_args)]
    

    # Matthew M- If StationOccupation/Station Fail != "None or No Fail/Temporary" then Abandoned should be set to "Yes"
    #- Message should read "Abandoned should be set to 'Yes' when Station Fail != 'None or No Fail' or 'Temporary'" # Adjusted 9/27/18 - Dario made this a warning not an error
    print("If StationOccupation/Station Fail != None or No Fail/Temporary then Abandoned should be set to Yes")
    results= eng.execute("select lu_stationfails.stationfail from lu_stationfails")
    lu_sf1 = pd.DataFrame(results.fetchall())
    lu_sf1.columns = results.keys()
    lu_sf1.columns = [x.lower() for x in lu_sf1.columns]
    lu_sf1=lu_sf1.stationfail[lu_sf1.stationfail.str.contains('None or No Failure|Temporary|Temporarily ', case=False, na=False)]
    print("lu_sf1")
    print(lu_sf1)
    print(occupation[(~occupation.stationfail.isin(lu_sf1.tolist())) & ~occupation['abandoned'].isin(['Yes', 'yes'])])
    occupation_args.update({
        "badrows": occupation[(~occupation.stationfail.isin(lu_sf1.tolist())) & ~occupation['abandoned'].isin(['Yes', 'yes'])].tmp_row.tolist(),
        "badcolumn": 'StationFail',
        "error_type": 'Undefined Warning',
        "error_message": 'If StationOccupation/StationFail is set to anything other than None or Temporary then Abandoned should be set to Yes.'
    })
    errs = [*errs, checkData(**occupation_args)]

    #2nd case check If StationOccupation/Station Fail = "None or No Fail/Temporary" then Abandoned should be set to "No"
    #- Message should read "Abandoned should be set to "No" when Station Fail is None or No Failure or Temporary"
    print("If StationOccupation/Station Fail = None or No Fail/Temporary then Abandoned should be set to No")
    print(occupation[(occupation.stationfail.isin(lu_sf1.tolist())) & occupation['abandoned'].isin(['Yes', 'yes'])])
    occupation_args.update({
        "badrows": occupation[(occupation.stationfail.isin(lu_sf1.tolist())) & occupation['abandoned'].isin(['Yes', 'yes'])].tmp_row.tolist(),
        "badcolumn": 'StationFail',
        "error_type": 'Undefined Warning',
        "error_message": 'If StationOccupation/StationFail is set to None or Temporary then Abandoned should be set to No.'
    })
    errs = [*errs, checkData(**occupation_args)]
    ### END OCCUPATION CHECKS ###

    

    # ------- Grab Checks ------- #
    print("Starting Grab Checks")
    ## jordan golemo - New calculated field (GrabDistanceToNominalTarget) . Look at Field Assignment Table target latitude/longitude. How are far off is Grab/Lat/Lon to target.
    print("##  New calculated field (GrabDistanceToNominalTarget) . Look at Field Assignment Table target latitude/longitude. How are far off is Grab/Lat/Lon to target ##")
    # create dataframe from Database field_assignment_table
    
    latlons = eng.execute('select stationid, targetlatitude, targetlongitude from field_assignment_table;')
    db = pd.DataFrame(latlons.fetchall())
    db.columns = latlons.keys()
    
    # creates list of station_ids in the current database
    db_list = db.stationid.tolist()
    # dataframe of grab stationid / latitude / longitude
    grab_locs = pd.DataFrame({'stationid':grab['stationid'],'glat':grab['latitude'],'glon':grab['longitude']})
    # makes sure submitted stationids are found in database
    grab_locs['validstations'] = grab_locs.stationid.apply(lambda row: True if row in db_list else np.nan) 
    print("make sure submitted stationids are found in database")
    print(grab.loc[grab_locs.validstations.isnull()])
    grab_args.update({
        "badrows": grab.loc[grab_locs.validstations.isnull()].tmp_row.tolist(),
        "badcolumn": 'StationID',
        "error_type": "Undefined Error",
        "error_message" : 'Could not match submitted StationID to field assignment table'
    })
    errs = [*errs, checkData(**grab_args)]
    
    # matches grab lat/lon to target lat/lon by stationid 
    print("matches grab lat/lon to target lat/lon by stationid")
    coords = pd.merge(grab_locs,db, how = 'left', on='stationid')
    coords.dropna(inplace=True)
    # creates new field "DistanceToNominalTarget" for Grab
    #grab['grabdistancetonominaltarget'] = pd.Series([-88]*(len(grab)))
    coords['targetlatitude'] = coords['targetlatitude'].apply(lambda x: float(x))
    coords['targetlongitude'] = coords['targetlongitude'].apply(lambda x: float(x))
    grab['grabdistancetonominaltarget']=haversine_np(coords['glon'],coords['glat'],coords['targetlongitude'],coords['targetlatitude'])
    grab['grabdistancetonominaltarget'] = grab['grabdistancetonominaltarget'].replace(np.nan,-88)
    print(grab.loc[grab.grabdistancetonominaltarget > 100])
    grab_args.update({
        "badrows": grab.loc[grab.grabdistancetonominaltarget > 100].tmp_row.tolist(),
        "badcolumn": 'Latitude,Longitude',
        "error_type": "Undefined Warning",
        "error_message" : 'Grab Distance to Nominal Target > 100m'
    })
    warnings = [*warnings, checkData(**grab_args)]


    # eric - check that Grab/Depth is more than 10% off of StationOccupation/Depth - warning only  - Will need to check database for StationOccupation whether user has provided or not. Same as trawl check.
    print("## Check that Grab/Depth is more than 10% off of StationOccupation/Depth - warning only  - Will need to check database for StationOccupation whether user has provided or not. Same as trawl check. ##")
    station_database = eng.execute('select stationid from field_assignment_table;')
    db = pd.DataFrame(station_database.fetchall())
    db.columns = station_database.keys()
    # new code added by paul based on trawl above - bug not distinguishing grab and trawl
    occupation_grab = occupation[['stationid','occupationdepth','collectiontype']].where(occupation['collectiontype'].isin(['Grab']))
    print("occupation_grab")
    print(occupation_grab)
    # drop emptys
    occupation_grab = occupation_grab.dropna()
    # now we get the correct number of grabs
    # table_occupation merges the stationid from the database and the table(occupation) from submission
    table_occupation = pd.merge(occupation_grab[['stationid','occupationdepth']], db[['stationid']], how = 'left', on = 'stationid')
    # table_grab merges the stationid from the database and the table(grab) from (errors.xlxs)
    table_grab = pd.merge(grab[['stationid', 'stationwaterdepth','tmp_row']], db[['stationid']], how = 'left', on = 'stationid')
    # table_occ_grab merges the table occupation and table_grab based on their stationid
    table_occ_grab = pd.merge(table_occupation[['stationid','occupationdepth']], table_grab[['stationid', 'stationwaterdepth','tmp_row']], how= 'right', on = 'stationid')
    print(table_occ_grab.loc[(abs(table_occ_grab['stationwaterdepth'] - table_occ_grab['occupationdepth'])/table_occ_grab['stationwaterdepth']*100) > 10])
    grab_args.update({
        "badrows": table_occ_grab.loc[(abs(table_occ_grab['stationwaterdepth'] - table_occ_grab['occupationdepth'])/table_occ_grab['stationwaterdepth']*100) > 10].tmp_row.tolist(),
        "badcolumn": 'StationWaterDepth',
        "error_type": "Undefined Warning",
        "error_message" : 'Grab StationWaterDepth is more than 10 percent off Occupation Depth'
    })
    warnings = [*warnings, checkData(**grab_args)]


    # Matthew M - Check that user has entered a comment if they selected a grabfail code that requires comment. See lu_grabfails, commentrequired field.
    print("## Check that user has entered a comment if they selected a grabfail code that requires comment. See lu_grabfails, commentrequired field. ##")
    results = eng.execute("select lu_grabfails.grabfail, lu_grabfails.commentrequired from lu_grabfails where commentrequired = 'yes';")
    lu_gf= pd.DataFrame(results.fetchall())
    lu_gf.columns=results.keys()
    lu_gf.columns = [x.lower() for x in lu_gf.columns]
    print("lu_gf:")
    print(lu_gf)
    print("grab.comments")
    print(grab['comments'])
    print(grab[(grab['grabfail'].isin(lu_gf.grabfail.tolist()))& (grab['comments'].isnull())])
    checkData(grab[(grab['grabfail'].isin(lu_gf.grabfail.tolist())) & (grab['comments'].isnull())].tmp_row.tolist(), 'Comments', 'Undefined Error', 'error', f'A comment is required for that stationfail option. Please see: <a href={current_app.script_root}/scraper?action=help&layer=lu_grabfails target=_blank>GrabFail lookup</a>.', grab)
    grab_args.update({
        "badrows": grab[(grab['grabfail'].isin(lu_gf.grabfail.tolist())) & (grab['comments'].isnull())].tmp_row.tolist(),
        "badcolumn": 'Comments',
        "error_type": "Undefined Error",
        "error_message" : f'A comment is required for that stationfail option. Please see: <a href=/{current_app.script_root}/scraper?action=help&layer=lu_grabfails target=_blank>GrabFail lookup</a>.'
    })
    errs = [*errs, checkData(**grab_args)]
    
    
    print("end grab CHECKS")
    ## end grab CHECKS ##

    return {'errors': errs, 'warnings': warnings}
