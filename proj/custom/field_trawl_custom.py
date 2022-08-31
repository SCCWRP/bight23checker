# Dont touch this file! This is intended to be a template for implementing new custom checks

from inspect import currentframe
from flask import current_app, g
from .functions import checkData, haversine_np, check_distance, check_time
import pandas as pd
import re
from shapely.geometry import Point, LineString
import numpy as np



def field_trawl(all_dfs):
    
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
    trawl = all_dfs['tbl_trawlevent']


    occupation_args = {
        "dataframe": occupation,
        "tablename": 'tbl_stationoccupation',
        "badrows": [],
        "badcolumn": "",
        "error_type": "",
        "is_core_error": False,
        "error_message": ""
    }

    trawl_args = {
        "dataframe": trawl,
        "tablename": 'tbl_trawlevent',
        "badrows": [],
        "badcolumn": "",
        "error_type": "",
        "is_core_error": False,
        "error_message": ""
    }

    # ------- LOGIC CHECKS ------- #
    # Check - Each Trawl record must have a corresponding stationoccupation record
    tmp = occupation.merge(trawl.assign(present = 'yes'), on = ['staitonid','sampledate','samplingorganization'], how = 'left')
    badrows = tmp[pd.isnull(tmp.present)].tmp_row.tolist()
    trawl_args.update({
      "badrows": badrows,
      "badcolumn": "StationID,SampleDate,SamplingOrganization",
      "error_type" : "Logic Error",
      "error_message" : "Each Trawl record must have a corresponding Occupation record. Records are matched on StationID, SampleDate, and SamplingOrganization."
    })
    errs = [*errs, checkData(**trawl_args)]
    
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
        checkTime(trawl, 'OverTime', trawl_args),
        checkTime(trawl, 'StartTime', trawl_args),
        checkTime(trawl, 'EndTime', trawl_args),
        checkTime(trawl, 'DeckTime', trawl_args),
        checkTime(trawl, 'OnBottomTime', trawl_args)
    ]
    # ------- END LOGIC CHECKS ------- #


    # ------- Occupation Checks ------- #
    ## Kristin - StationOccupation/Trawl/Grab check DepthUnits field make sure nobody has entered feet instead of meters
	## (this is an error not a warning). Generic lookup list allows it, but Dario doesnt want users to be able to enter feet. 
    # Depth units should be in meters, not feet
    badrows = (occupation[['occupationdepthunits','tmp_row']].where(occupation['occupationdepthunits'].isin(['ft','f'])).dropna()).tmp_row.tolist()
    occupation_args.update({
        "badrows": badrows,
        "badcolumn": 'OccupationDepthUnits',
        "error_type" : "Undefined Error",
        "error_message" : "OccupationDepthUnits should be in meters, not feet"
    })
    errs = [*errs, checkData(**occupation_args)]

    # Depth units should be in meters, not feet
    badrows = (trawl[['depthunits','tmp_row']].where(trawl['depthunits'].isin(['ft','f'])).dropna()).tmp_row.tolist()
    trawl_args.update({
        "badrows": badrows,
        "badcolumn": 'DepthUnits',
        "error_type" : "Undefined Error",
        "error_message" : "DepthUnits should be in meters, not feet"
    })
    errs = [*errs, checkData(**trawl_args)]
    

    # Comment required if the station was abandoned
    badrows = occupation[['abandoned', 'comments','tmp_row']].where(occupation['abandoned'].isin(['Yes'])).dropna(axis = 0, how = 'all').loc[pd.isnull(occupation['comments'])].tmp_row.tolist()
    occupation_args.update({
        "badrows": badrows,
        "badcolumn": 'Comments',
        "error_type" : "Missing Required Data",
        "error_message" : 'A comment is required if the station was abandoned'
    })
    errs = [*errs, checkData(**occupation_args)]

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
        strats = pd.merge(occupation[['stationid','salinity']],estuaries, how = 'left', on='stationid')
        occupation_args.update({
            "badrows": strats[pd.isnull(strats.salinity)].tmp_row.tolist(),
            "badcolumn": 'Salinity',
            "error_type": 'Undefined Error',
            "error_message": 'Station in Estuary or Brackish Estuary. Salinity is required and user must enter -88 if measurement is missing.'
        })
        errs = [*errs, checkData(**occupation_args)]



    # Jordan - Station Occupation Latitude/Longitude should be no more than 100M from Field Assignment Table Target Latitude/Longitude otherwise warning
    # Merges SO dataframe and FAT dataframe according to StationIDs
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

    # ------- Trawl Checks ------- #
    # Eric Hermoso - (TrawlOverDistance) - Distance from net start (Trawl/OverLat/OverLon) to end (Trawl/StartLat/StartLon) in meters.
    print('New calculated field (TrawlOverDistance) - Distance from net start (Trawl/OverLat/OverLon) to end (Trawl/StartLat/StartLon) in meters.')
    trawl['trawloverdistance'] = check_distance(trawl,trawl['overlatitude'],trawl['startlatitude'],trawl['overlongitude'],trawl['startlongitude'])

    # Eric Hermoso - (TrawlDeckDistance) - Distance from end (Trawl/EndLat/EndLon) to on-deck (Trawl/DeckLat/DeckLon) in meters)
    print('New calculated field (TrawlDeckDistance)')
    trawl['trawldeckdistance'] = check_distance(trawl,trawl['endlatitude'],trawl['decklatitude'],trawl['endlongitude'],trawl['decklongitude'])

    # Kristin - New calculated field (TrawlDistance) - Distance from start (Trawl/StartLat/StartLon) to end (Trawl/EndLat/EndLon) in meters.
    print('New calculated field (TrawlDistance)')
    trawl['trawldistance'] = check_distance(trawl,trawl['startlatitude'],trawl['endlatitude'],trawl['startlongitude'],trawl['endlongitude'])

    # (TrawlOverTime) - Time between net start (Trawl/OverDate) to end (Trawl/StartDate) in meters
    print('New calculated field (TrawlOverTime) - Time between net start (Trawl/OverDate) to end (Trawl/StartDate) in meters')
    trawl['trawlovertime'] = check_time(trawl['overtime'].map(str),trawl['starttime'].map(str))

    # (TrawlDeckTime) - Time between end (Trawl/EndDate) to on-deck (Trawl/DeckDate) in meters
    print('New calculate field (TrawlDeckTime) - Time between end (Trawl/EndDate) to on-deck (Trawl/DeckDate) in meters')
    trawl['trawldecktime'] = check_time(trawl['endtime'].map(str),trawl['decktime'].map(str))

    # (TrawlTimeToBottom) - Distance from (Trawl/WireOut) divided by calculated field OverDistance
    print('New calculate field (TrawlTimeToBottom)')
    # If trawl['wireout'] is equal to 0, changes to -88 so that trawltimetobottom is numeric and not null
    trawl['wireout'] = [ -88 if trawl['wireout'][x]== 0 else trawl['wireout'][x] for x in trawl.index]
    trawl['trawltimetobottom'] = trawl['trawloverdistance']/trawl['wireout']

    # Kristin - New calculated field (TrawlTime) - Time difference from start (Trawl/StartDate) to end (Trawl/EndDate).
    print('before trawltime')
    trawl['trawltime'] = check_time(trawl.starttime.map(str), trawl.endtime.map(str))



    # Kristin - Check if Trawl/TrawlNumber is out of sequence based upon SampleDate and OverTime. - bug fix 26jun18
    print("## Check if Trawl/TrawlNumber is out of sequence based upon SampleDate and OverTime. ##")
    stations = trawl[['stationid','sampledate','overtime','trawlnumber','tmp_row']].copy()
    print('User must submit more than one station to run this check')
    if len(stations) > 1:
        # creates dataframe consisting of trawl submissions with more than one trawl
        station_duplicates = pd.DataFrame(stations[stations.stationid.isin(stations.stationid[stations.stationid.duplicated()])])
        # makes sure that both sampledate and overtime are string values
        station_duplicates['sampledate'] = station_duplicates['sampledate'].map(str)
        station_duplicates['overtime'] = station_duplicates.overtime.map(str)
        print('Trawl submissions with more than one trawl:')
        print(station_duplicates)
        if len(station_duplicates) > 1 :
            # creates a datetime value from sampledate and overtime
            station_duplicates['datetime'] = pd.to_datetime(station_duplicates['sampledate'] + ' ' + station_duplicates['overtime'])
            station_duplicates['sequence'] = station_duplicates.groupby('stationid')['datetime'].rank(ascending=1)
            print('Trawl submissions ranked by their date and time')
            print(station_duplicates)
            trawl_args.update({
                "badrows": station_duplicates.loc[station_duplicates.trawlnumber != station_duplicates.sequence].tmp_row.tolist(),
                "badcolumn": "TrawlNumber",
                "error_type": "Undefined Error",
                "error_message" :'TrawlNumber sequence is incorrect, check SampleDate and OverTime.'
            })
            errs = [*errs, checkData(**trawl_args)]

    # Eric Hermoso - Check that both Trawl/StartDepth and Trawl/EndDepth are no more than 10% off of StationOccupation (Depth) - warning only 
    # lets just get actual trawls from station occupation - we may to adjust this further to only get successful trawls
    print("## Check that both Trawl/StartDepth and Trawl/EndDepth are no more than 10% off of StationOccupation (Depth) - warning only ##")
    occupation_trawls = occupation[['stationid','occupationdepth','collectiontype']].where(occupation['collectiontype'].isin(['Trawl 10 Minutes','Trawl 5 Minutes']))
    print("occupation_trawls")
    print(occupation_trawls)
    # drop emptys
    occupation_trawls = occupation_trawls.dropna()
    # now we get the correct number of trawls
    merge_trawl_occupation = pd.merge(occupation_trawls[['stationid','occupationdepth']], trawl[['stationid','startdepth','enddepth','tmp_row']], how = 'right', on = 'stationid')
    print("merge_trawl_occupation")
    print(merge_trawl_occupation)
    print("## Trawl start depth is greater than 10 percent of occupation depth ##")
    trawl_args.update({
        "badrows": merge_trawl_occupation.loc[(abs(merge_trawl_occupation['occupationdepth'] - merge_trawl_occupation['startdepth'])/merge_trawl_occupation['startdepth']*100) > 10].tmp_row.tolist(),
        "badcolumn": "StartDepth",
        "error_type": "Undefined Warning",
        "error_message" : 'Trawl start depth is greater than 10 percent of occupation depth.'
    })
    warnings = [*warnings, checkData(**trawl_args)]

    print("## Trawl end depth is greater than 10 percent of occupation depth ##")
    trawl_args.update({
        "badrows": merge_trawl_occupation.loc[(abs(merge_trawl_occupation['occupationdepth'] - merge_trawl_occupation['enddepth'])/merge_trawl_occupation['enddepth']*100) > 10 ].tmp_row.tolist(),
        "badcolumn": "EndDepth",
        "error_type": "Undefined Warning",
        "error_message" : 'Trawl end depth is greater than 10 percent of occupation depth.'
    })
    warnings = [*warnings, checkData(**trawl_args)]

    # 2 - Kristin - 
    # If its a 10 minute trawl the distance should be greater than 650 meters otherwise warn.
    # If its a 5 minute trawl the distance should be greater than 325 meters otherwise warn.
    # If 10 minute trawl is greater than 16 minutes or less than 8 then warning.
    # If 5 minute trawl is greater than 8 minutes or less than 4 then warning.
    # use occupation trawls from check above - has the same content you need
    trawl_occupation_time = pd.merge(occupation_trawls[['stationid','collectiontype']], trawl[['stationid','trawltime','trawldistance','tmp_row']], how = 'right', on = 'stationid')
    print("## CHECK 10 MINUTE TRAWL THE DISTANCE SHOULD BE GREATER THAN 650 METERS ##")
    print(trawl_occupation_time)
    trawl_args.update({
        "badrows": trawl_occupation_time.loc[(trawl_occupation_time['collectiontype']=='Trawl 10 Minutes')&(trawl_occupation_time['trawldistance']< 650)].tmp_row.tolist(),
        "badcolumn": 'TrawlDistance',
        "error_type": "Undefined Warning",
        "error_message" : 'A 10 minute trawl should be greater than 650 m'
    })
    warnings = [*warnings, checkData(**trawl_args)]
    
    print("## CHECK 5 MINUTE TRAWL THE DISTANCE SHOULD BE GREATER THAN 325 METERS ##")
    trawl_args.update({
        "badrows": trawl_occupation_time.loc[(trawl_occupation_time['collectiontype']==5)&(trawl_occupation_time['trawldistance'] < 325)].tmp_row.tolist(),
        "badcolumn": 'TrawlDistance',
        "error_type": "Undefined Warning",
        "error_message" : 'A 5 minute trawl should be greater than 325 m'
    })
    warnings = [*warnings, checkData(**trawl_args)]
    
    print("## CHECK 10 MINUTE TRAWL SHOULD NOT RUN LONGER THAN 16 MINUTES OR SHORTER THAN 8 ##")
    trawl_args.update({
        "badrows": trawl_occupation_time.loc[(trawl_occupation_time['collectiontype']=='Trawl 10 Minutes')&((trawl_occupation_time['trawltime'] < 8)|(trawl_occupation_time['trawltime'] > 16))].tmp_row.tolist(),
        "badcolumn": 'TrawlTime',
        "error_type": "Undefined Warning",
        "error_message" : 'A 10 minute trawl should be between 8 and 16 minutes'
    })
    warnings = [*warnings, checkData(**trawl_args)]
    
    print("## CHECK 5 MINUTE TRAWL SHOULD NOT RUN LONGER THAN 8 MINUTES OR SHORTER THAN 4 MINUTES ##")
    trawl_args.update({
        "badrows": trawl_occupation_time.loc[(trawl_occupation_time['collectiontype']=='Trawl 5 Minutes')&((trawl_occupation_time['trawltime'] < 4)&(trawl_occupation_time['trawltime']> 8))].tmp_row.tolist(),
        "badcolumn": 'TrawlTime',
        "error_type": "Undefined Warning",
        "error_message" : 'A 5 minute trawl should be between 4 and 8 minutes'
    })
    warnings = [*warnings, checkData(**trawl_args)]

    ## 3 - Jordan Golemo -
    ##  New calculated field (TrawlDistanceToNominalTarget) - Draw a line from StartLat/StartLon to EndLat/Lon calculate nearest point to tblStations Lat/Lon.
    ##  This check is only to be done for submissions where the trawl track table hasn't been provided (most of the time).
    print("New calculated field (TrawlDistanceToNominalTarget) - Draw a line from StartLat/StartLon to EndLat/Lon calculate nearest point to tblStations Lat/Lon")
    # creates dataframe from field assignment table
    field_sql = eng.execute("select stationid,targetlatitude,targetlongitude from field_assignment_table;")
    station = pd.DataFrame(field_sql.fetchall())
    station.columns = field_sql.keys()
    # creates new dataframes containing pertinent fields
    td = trawl[['stationid','startlatitude','startlongitude','endlatitude','endlongitude','tmp_row']]
    sd = pd.DataFrame({'stationid':station['stationid'],'stlat':station['targetlatitude'],'stlon':station['targetlongitude']})
    dt = pd.merge(td, sd, how = 'left', on ='stationid')
    # Adds error for unmatched trawls
    print("Adds error for unmatched trawls")
    trawl_args.update({
        "badrows": trawl.loc[dt['stlat'].isnull()].tmp_row.tolist(),
        "badcolumn": 'StationID',
        "error_type": "Undefined Warning",
        "error_message" : 'Could not match submitted StationID to field assignment table'
    })
    warnings = [*warnings, checkData(**trawl_args)]
    
    dt = dt.dropna()
    # initializes new field "TrawlDistanceToNominalTarget"
    trawl['trawldistancetonominaltarget'] = pd.Series([-88]*(len(trawl)))
    # determines closest point on trawl to station and determines min distance
    for i in dt.index:
        stloc = Point(dt['stlat'][i],dt['stlon'][i])
        line = LineString([(dt['startlatitude'][i],dt['startlongitude'][i]),(dt['endlatitude'][i],dt['endlongitude'][i])])
        closest_point = line.interpolate(line.project(stloc))
        trawl.loc[i,'trawldistancetonominaltarget'] = haversine_np(closest_point.y,closest_point.x,stloc.y,stloc.x)
    # Adds error for trawls over 100m away from Station Location, and missing lat/lon entries
    print("Adds error for trawls over 100m away from Station Location and Missiong Lat/Lon Entries.")
    trawl['trawldistancetonominaltarget'].dropna(inplace=True)
    trawl_args.update({
        "badrows": trawl.loc[trawl['trawldistancetonominaltarget']>100].tmp_row.tolist(),
        "badcolumn": 'startlatitude,startlongitude,endlatitude,endlongitude',
        "error_type": "Undefined Warning",
        "error_message" : 'Trawl path over 100m away from Station Location'
    })
    warnings = [*warnings, checkData(**trawl_args)]
    
    
    missing_entries = [dt.loc[dt[c].isnull()] for c in td.columns]
    trawl_args.update({
        "badrows": missing_entries[len(missing_entries) > 0].tmp_row.tolist(),
        "badcolumn": 'TrawlDistanceToNominalTarget',
        "error_type": "Undefined Warning",
        "error_message" :'Missing Lat/Lon entries'
    })
    warnings = [*warnings, checkData(**trawl_args)]



    ## 4 - Kristin - bug fixed on 26jun18
    ## Check - A comment is required if TrawlFail is equal to Other
    print("## A COMMENT IS REQUIRED IF TRAWLAILCODE IS EQUAL TO OTHER ##")
    trawlcode = trawl[['trawlfail', 'comments','tmp_row']].where(trawl['trawlfail'].isin(['Other trawl failure'])).dropna(axis = 0, how = 'all')
    trawl_args.update({
        "badrows": trawlcode.loc[pd.isnull(trawlcode['comments'])].tmp_row.tolist(),
        "badcolumn": 'TrawlFail',
        "error_type": "Undefined Error",
        "error_message" : 'A comment is required if trawlfail is equal to other'
    })
    errs = [*errs, checkData(**trawl_args)]

    ## Kristin - bug fixed on 26jun18
    ## Check - If PTSensor = Yes then PTSensorManufacturer required (but not SerialNumber/OnBottomTemp/OnBottomRules for table on OnBottomTemp/OnBottomTime incorrect need to be adjusted to not required)
    print('## PTSENSOR MANUFACTURER REQUIRED IF PT SENSOR IS YES##')
    print(trawl[(trawl.ptsensor == 'Yes')&(trawl.ptsensormanufacturer.isnull())].tmp_row.tolist())
    trawl_args.update({
        "badrows": trawl[(trawl.ptsensor == 'Yes')&(trawl.ptsensormanufacturer.isnull())].tmp_row.tolist(),
        "badcolumn": 'PTSensorManufacturer',
        "error_type": "Undefined Error",
        "error_message" : 'PT Sensor Manufacturer required if PT Sensor is Yes'
    })
    errs = [*errs, checkData(**trawl_args)]


    ## Jordan - If PTSensor = Yes then PTSensorSerialNumber required. Added 9/18/18
    print('## PTSENSOR SERIALNUMBER REQUIRED IF PT SENSOR IS YES##')
    print(trawl[(trawl.ptsensor == 'Yes')&(trawl.ptsensorserialnumber.isnull())].tmp_row.tolist())
    trawl_args.update({
        "badrows": trawl[(trawl.ptsensor == 'Yes')&(trawl.ptsensormanufacturer.isnull())].tmp_row.tolist(),
        "badcolumn": 'PTSensorManufacturer',
        "error_type": "Undefined Error",
        "error_message" : 'PT Sensor Serial Number is required if PT Sensor is Yes'
    })
    errs = [*errs, checkData(**trawl_args)]

    #Matthew M- Check that user has entered a comment if they selected a trawlfail code that requires comment. See lu_trawlfails, commentrequired field
    print("## Check that user has entered a comment if they selected a trawlfail code that requires comment. See lu_trawlfails, commentrequired field. ##")
    results= eng.execute("select lu_trawlfails.trawlfailure, lu_trawlfails.commentrequired from lu_trawlfails where commentrequired = 'Yes';")
    lu_tf= pd.DataFrame(results.fetchall())
    lu_tf.columns=results.keys()
    lu_tf.columns = [x.lower() for x in lu_tf.columns]
    print(trawl[(trawl['trawlfail'].isin(lu_tf.trawlfailure.tolist()))& (trawl['comments'].isnull())])
    trawl_args.update({
        "badrows": trawl[(trawl['trawlfail'].isin(lu_tf.trawlfailure.tolist())) & (trawl['comments'].isnull())].tmp_row.tolist(),
        "badcolumn": 'Comments',
        "error_type": "Undefined Error",
        "error_message" : 'A comment is required for that stationfail option. Please see: <a href=http://checker.sccwrp.org/checker/scraper?action=help&layer=lu_trawlfails target=_blank>TrawlFail lookup</a>.'
    })
    errs = [*errs, checkData(**trawl_args)]

    # ------- END Trawl Checks ------- #

    return {'errors': errs, 'warnings': warnings}
