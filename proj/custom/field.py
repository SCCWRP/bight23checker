# this function will be imported into field_trawl, and field_grab, and field_trawl_and_grab
# This way, one function will control the field checks
# This system will be unique to bight, since it is one datatype, with three different possible combinations of dataframes
# We previously had 3 custom checks files with redundant code, but instead, we can do it this way:
#  Make this file with a function that gets imported to those 3 other custom checks files
#  This function can take 3 dataframes as arguments, with default on each set to none. But the occupation dataframe must be required.

from inspect import currentframe
from flask import current_app, g, session
from .functions import checkData, haversine_np, check_distance, check_time, check_strata_grab, check_strata_trawl, export_sdf_to_json
import pandas as pd
import re
from shapely.geometry import Point, LineString
import numpy as np
from arcgis.gis import GIS
from arcgis.features import GeoAccessor, GeoSeriesAccessor
from arcgis.geometry.filters import within, contains
from arcgis.geometry import lengths, areas_and_lengths, project
import os

def fieldchecks(occupation, eng, trawl = None, grab = None):
    
    current_function_name = str(currentframe().f_code.co_name)
    print("current_function_name")
    print(current_function_name)

    print("occupation")
    print(occupation)
    print("grab")
    print(grab)
    print("trawl")
    print(trawl)
    
    # define errors and warnings list
    # These will be returned from the function at the end
    errs = []
    warnings = []

    args = {
        "badrows": [],
        "badcolumn": "",
        "error_type": "",
        "is_core_error": False,
        "error_message": ""
    }

    # Sorry for the weird notation i just wanted this to take fewer lines
    occupation_args = {**args, **{"dataframe": occupation, "tablename": 'tbl_stationoccupation' } }
    trawl_args      = {**args, **{"dataframe": trawl,      "tablename": 'tbl_trawlevent'        } }
    grab_args       = {**args, **{"dataframe": grab,       "tablename": 'tbl_grabevent'         } }

    # Initiates the parts needed for strata check
    gis = GIS(os.environ.get("ARCGIS_API_URL"),os.environ.get("ARCGIS_API_USERNAME"),os.environ.get("ARCGIS_API_PASSWORD"))
    # Query Strata Bight 2018
    strata = gis.content.get(os.environ.get("BIGHT18_STRATA_LAYER_ID")).layers[0].query().sdf

    # Convert from spatial reference from 3857 to 4326
    strata['SHAPE'] = pd.Series(project(geometries=strata['SHAPE'].tolist(), in_sr=3857, out_sr=4326))
    
    # Turn the dataframe strata into a dictionary so we can look it up later when we check if points are in polygon
    strata_lookup = {}
    for tup, subdf in strata.groupby(['region','stratum']):
        # For some reasons, the stratum in stations_grab_final is Bay, but it's Bays in the strata layer
        if tup[1] == 'Bay':
            tup = (tup[0], 'Bays')
        strata_lookup[(tup[0],tup[1])] = subdf['SHAPE'].iloc[0]
    
    eng = g.eng
    
    field_assignment_table = pd.read_sql("SELECT * FROM field_assignment_table", eng)

    # ------- LOGIC CHECKS ------- #
    print("# ------- LOGIC CHECKS ------- #")
    
    # If its a field grab submission, there should be only grab collectiontype records.
    if trawl is None:
        occupation_args.update({
            "badrows": occupation[occupation.collectiontype != 'Grab'].index.tolist(),
            "badcolumn": "CollectionType",
            "error_type" : "Logic Error",
            "error_message" : "This is a Field Occupation/Grab submission, but there are records that do not have a collection type of 'Grab'"
        })
        errs.append(checkData(**occupation_args))
    
    # If its a field trawl submission, there should be only trawl collectiontype records.
    if grab is None:
        occupation_args.update({
            "badrows": occupation[~occupation.collectiontype.isin(['Trawl 5 Minutes', 'Trawl 10 Minutes'])].index.tolist(),
            "badcolumn": "CollectionType",
            "error_type" : "Logic Error",
            "error_message" : "This is a Field Occupation/Trawl submission, but there are records that do not have a collection type of 'Trawl 5 Minutes' or 'Trawl 10 Minutes'"
        })
        errs.append(checkData(**occupation_args))


    if trawl is not None:
        # Check - Each Trawl record must have a corresponding stationoccupation record
        print("# Check - Each Trawl record must have a corresponding stationoccupation record") 
        print("with collection type 'Trawl 5 Minutes' or 'Trawl 10 Minutes'")
        tmpocc = occupation[occupation.collectiontype.isin(['Trawl 5 Minutes', 'Trawl 10 Minutes'])].assign(present = 'yes')
        if not tmpocc.empty:
            tmp = trawl.merge(
                tmpocc, 
                left_on = ['stationid','sampledate','samplingorganization'], 
                right_on = ['stationid','occupationdate','samplingorganization'], 
                how = 'left',
                suffixes = ('','_occ')
            )
            badrows = tmp[pd.isnull(tmp.present)].tmp_row.tolist()
            trawl_args.update({
                "badrows": badrows,
                "badcolumn": "StationID,SampleDate,SamplingOrganization",
                "error_type" : "Logic Error",
                "error_message" : "Each Trawl record must have a corresponding Occupation record (with a Trawl collectiontype). Records are matched on StationID, SampleDate, and SamplingOrganization."
            })
            errs = [*errs, checkData(**trawl_args)]
        else:
            occupation_args.update({
                "badrows": occupation.index.tolist(),
                "badcolumn": "CollectionType",
                "error_type" : "Logic Error",
                "error_message" : "There are no records with a collectiontype of 'Trawl 5/10 Minutes' although this is a Field Trawl submission"
            })
            errs.append(checkData(**occupation_args))
    
    if grab is not None:
        # Check - Each Grab record must have a corresponding stationoccupation record
        print("# Check - Each Grab record must have a corresponding stationoccupation record")
        print("with collection type Grab")
        tmpocc = occupation[occupation.collectiontype == 'Grab'].assign(present = 'yes')
        if not tmpocc.empty:
            tmp = grab.merge(
                tmpocc, 
                left_on = ['stationid','sampledate','samplingorganization'], 
                right_on = ['stationid','occupationdate','samplingorganization'], 
                how = 'left',
                suffixes = ('','_occ')
            )
            badrows = tmp[pd.isnull(tmp.present)].tmp_row.tolist()
            grab_args.update({
                "badrows": badrows,
                "badcolumn": "StationID,SampleDate,SamplingOrganization",
                "error_type" : "Logic Error",
                "error_message" : "Each Grab record must have a corresponding Occupation record (with a Grab collectiontype). Records are matched on StationID, SampleDate, and SamplingOrganization."
            })
            errs = [*errs, checkData(**grab_args)]
            del tmp
            del badrows
        else:
            occupation_args.update({
                "badrows": occupation.index.tolist(),
                "badcolumn": "CollectionType",
                "error_type" : "Logic Error",
                "error_message" : "There are no records with a collectiontype of 'Grab' although this is a Field Grab submission"
            })
            errs.append(checkData(**occupation_args))



    print("# Check the time formats on all time columns")
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
    
    print("# Grab and trawl may possibly be NoneTypes")
    # Grab and trawl may possibly be NoneTypes
    errs = [
        *errs, 
        checkTime(occupation, 'OccupationTime', occupation_args),
        checkTime(trawl, 'OverTime', trawl_args) if trawl is not None else {} ,
        checkTime(trawl, 'StartTime', trawl_args) if trawl is not None else {},
        checkTime(trawl, 'EndTime', trawl_args) if trawl is not None else {},
        checkTime(trawl, 'DeckTime', trawl_args) if trawl is not None else {},
        checkTime(trawl, 'OnBottomTime', trawl_args) if trawl is not None else {},
        checkTime(grab, 'SampleTime', grab_args) if grab is not None else {}
    ]
    # ------- END LOGIC CHECKS ------- #


    print("# ------- Occupation Checks ------- #")
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

    if trawl is not None:
        print("# Depth units should be in meters, not feet")
        # Depth units should be in meters, not feet
        badrows = (trawl[['depthunits','tmp_row']].where(trawl['depthunits'].isin(['ft','f'])).dropna()).tmp_row.tolist()
        trawl_args.update({
            "badrows": badrows,
            "badcolumn": 'DepthUnits',
            "error_type" : "Undefined Error",
            "error_message" : "DepthUnits should be in meters, not feet"
        })
        errs = [*errs, checkData(**trawl_args)]
    
    if grab is not None:
        print("# Depth units should be in meters, not feet")
        # Depth units should be in meters, not feet
        badrows = (grab[['stationwaterdepthunits','tmp_row']].where(grab['stationwaterdepthunits'].isin(['ft','f'])).dropna()).tmp_row.tolist()
        grab_args.update({
            "badrows": badrows,
            "badcolumn": 'StationWaterDepthUnits',
            "error_type" : "Undefined Error",
            "error_message" : "DepthUnits should be in meters, not feet"
        })
        errs = [*errs, checkData(**grab_args)]


    print("# Comment required if the station was abandoned")
    # Comment required if the station was abandoned
    badrows = occupation[['abandoned', 'comments','tmp_row']].where(occupation['abandoned'].isin(['Yes'])).dropna(axis = 0, how = 'all').loc[pd.isnull(occupation['comments'])].tmp_row.tolist()
    occupation_args.update({
        "badrows": badrows,
        "badcolumn": 'Comments',
        "error_type" : "Missing Required Data",
        "error_message" : 'A comment is required if the station was abandoned'
    })
    errs = [*errs, checkData(**occupation_args)]

    print("# Comment required for certain stationfail values")
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


    print("# Make sure agency was assigned to that station for the corresponding collection type - Grab or Trawl")
    print("# There should only be one sampling organization per submission - this is just a warning")
    sampling_organizations = occupation.samplingorganization.unique() 
    if len(sampling_organizations) >= 1:
        if len(sampling_organizations) > 1:
            occupation_args.update({
                "badrows": occupation[occupation.samplingorganization == occupation.samplingorganization.min()].tmp_row.tolist(),
                "badcolumn": 'SamplingOrganization',
                "error_type" : "Undefined Warning",
                "error_message" : "More than one agency detected"
            })
            warnings = [*warnings, checkData(**occupation_args)]
        
        for organization in sampling_organizations:
            trawlstations = pd.read_sql(f"SELECT DISTINCT stationid FROM field_assignment_table WHERE trawlagency = '{organization}'", eng).stationid.tolist()
            badrows = occupation[(occupation.collectiontype != 'Grab') & (~occupation.stationid.isin(trawlstations))].tmp_row.tolist()
            occupation_args.update({
                "badrows": badrows,
                "badcolumn": 'StationID,SamplingOrganization',
                "error_type" : "Undefined Warning",
                "error_message" : f"The organization {organization} was not assigned to trawl at this station"
            })
            warnings = [*warnings, checkData(**occupation_args)]
            
            grabstations = pd.read_sql(f"SELECT DISTINCT stationid FROM field_assignment_table WHERE grabagency = '{organization}'", eng).stationid.tolist()
            badrows = occupation[(occupation.collectiontype == 'Grab') & (~occupation.stationid.isin(grabstations))].tmp_row.tolist()
            occupation_args.update({
                "badrows": badrows,
                "badcolumn": 'StationID,SamplingOrganization',
                "error_type" : "Undefined Warning",
                "error_message" : f"The organization {organization} was not assigned to grab at this station"
            })
            warnings = [*warnings, checkData(**occupation_args)]
    else: 
        raise Exception("No sampling organization detected")

    print("# Check StationOccupation/Salinity - if the station is an Estuary or Brackish Estuary then the salinity is required")
    estuaries = pd.read_sql("SELECT stationid, stratum FROM field_assignment_table WHERE stratum IN ('Estuaries', 'Brackish Estuaries');", eng)

    print("# Only run if they submitted data for estuaries")
    if len((occupation[(occupation.stationid.isin(estuaries.stationid))]))!=0 :
        print("# for matching stationids, make sure Estuary and Brackish Estuary salinity has a value")
        print('## Make sure Estuary and Brackish Estuary salinity value is non-empty ##')
        strats = pd.merge(occupation[['stationid','salinity','tmp_row']],estuaries, how = 'left', on='stationid')
        occupation_args.update({
            "badrows": strats[pd.isnull(strats.salinity)].tmp_row.tolist(),
            "badcolumn": 'Salinity',
            "error_type": 'Undefined Error',
            "error_message": 'Station in Estuary or Brackish Estuary. Salinity is required and user must enter -88 if measurement is missing.'
        })
        errs = [*errs, checkData(**occupation_args)]



    print("# Jordan - Station Occupation Latitude/Longitude should be no more than 100M from Field Assignment Table Target Latitude/Longitude otherwise warning")
    print("# Merges SO dataframe and FAT dataframe according to StationIDs")
    so = occupation[['stationid','occupationlatitude','occupationlongitude','tmp_row']]
    fat = pd.read_sql("SELECT * FROM field_assignment_table", eng)
    sofat = pd.merge(so, fat, how = 'left', on ='stationid')

    # Raises Error for Unmatched StationIDs & Distances More than 100M from FAT Target
    print("Raises error for unmatched stationids & distances more than 100m from fat target:")
    print(sofat[sofat['targetlatitude'].isnull()])
    occupation_args.update({
        "badrows": sofat[sofat['targetlatitude'].isnull()].tmp_row.tolist(),
        "badcolumn": 'StationID',
        "error_type": 'Logic Error',
        "error_message": 'StationOccupation distance to target check - Could not find StationID in field assignment table.'
    })
    errs = [*errs, checkData(**occupation_args)]


    # Calculates distance between SO Lat/Lon and FAT Lat/Lon according to StationIDs
    print("Calculates distance between so lat/lon and fat lat/lon according to stationids:")

    # Need to specify the subset - it was dropping records it wasnt supposed to before
    sofat.dropna(subset=['targetlatitude','targetlongitude'], inplace=True)
    sofat['targetlatitude'] = sofat['targetlatitude'].apply(lambda x: float(x))
    sofat['targetlongitude'] = sofat['targetlongitude'].apply(lambda x: float(x))
    

    # https://stackoverflow.com/questions/29545704/fast-haversine-approximation-python-pandas
    # haversine apparently uses a projection other than WGS84 which may cause small errors but none significant enough to affect this check
    # plus this check is just a warning
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
    warnings = [*warnings, checkData(**occupation_args)]
    

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
        "error_type": 'Undefined Error',
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
        "error_type": 'Undefined Error',
        "error_message": 'If StationOccupation/StationFail is set to None or Temporary then Abandoned should be set to No.'
    })
    errs = [*errs, checkData(**occupation_args)]
    ### END OCCUPATION CHECKS ###
    
    
    if trawl is not None:
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
        badrows = [int(x) for x in merge_trawl_occupation.loc[(abs(merge_trawl_occupation['occupationdepth'] - merge_trawl_occupation['enddepth'])/merge_trawl_occupation['enddepth']*100) > 10 ].tmp_row.unique()]
        trawl_args.update({
            "badrows": badrows,
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
            "badcolumn": 'StartLatitude,StartLongitude,EndLatitude,EndLongitude',
            "error_type": "Undefined Warning",
            "error_message" : 'A 10 minute trawl should be greater than 650 m'
        })
        warnings = [*warnings, checkData(**trawl_args)]
        
        print("## CHECK 5 MINUTE TRAWL THE DISTANCE SHOULD BE GREATER THAN 325 METERS ##")
        trawl_args.update({
            "badrows": trawl_occupation_time.loc[(trawl_occupation_time['collectiontype']=='Trawl 5 Minutes')&(trawl_occupation_time['trawldistance'] < 325)].tmp_row.tolist(),
            "badcolumn": 'StartLatitude,StartLongitude,EndLatitude,EndLongitude',
            "error_type": "Undefined Warning",
            "error_message" : 'A 5 minute trawl should be greater than 325 m'
        })
        warnings = [*warnings, checkData(**trawl_args)]
        
        print("## CHECK 10 MINUTE TRAWL SHOULD NOT RUN LONGER THAN 16 MINUTES OR SHORTER THAN 8 ##")
        badrows = [int(x) for x in trawl_occupation_time.loc[(trawl_occupation_time['collectiontype']=='Trawl 10 Minutes')&((trawl_occupation_time['trawltime'] < 8)|(trawl_occupation_time['trawltime'] > 16))].tmp_row.unique()]
        trawl_args.update({
            "badrows": badrows,
            "badcolumn": 'StartTime,EndTime',
            "error_type": "Undefined Warning",
            "error_message" : 'A 10 minute trawl should be between 8 and 16 minutes'
        })
        warnings = [*warnings, checkData(**trawl_args)]
        
        print("## CHECK 5 MINUTE TRAWL SHOULD NOT RUN LONGER THAN 8 MINUTES OR SHORTER THAN 4 MINUTES ##")
        badrows = [int(x) for x in trawl_occupation_time.loc[(trawl_occupation_time['collectiontype']=='Trawl 5 Minutes')&((trawl_occupation_time['trawltime'] < 4)|(trawl_occupation_time['trawltime']> 8))].tmp_row.unique()]
        trawl_args.update({
            "badrows": badrows,
            "badcolumn": 'StartTime,EndTime',
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
            "error_message" : f'A comment is required for that stationfail option. Please see: <a href=/{current_app.script_root}/scraper?action=help&layer=lu_trawlfails target=_blank>TrawlFail lookup</a>.'
        })
        errs = [*errs, checkData(**trawl_args)]

        # Check if trawl stations are in strata
        print("Check if trawl stations are in strata")
        # checker thing is breaking 
        print("-------------------- it broke between this --------------------")
        bad_df = check_strata_trawl(trawl, strata_lookup, field_assignment_table)
        print("-------------------- and this --------------------") # yes it is breaking here :/

        if len(bad_df) > 0:
            export_sdf_to_json(os.path.join(session['submission_dir'], "bad_trawl.json"), bad_df)
            export_sdf_to_json(os.path.join(session['submission_dir'], "bight_strata.json"), strata[strata['region'].isin(bad_df['region'])])
        
        trawl_args.update({
            "badrows": bad_df.tmp_row.tolist(),
            "badcolumn": 'startlatitude,startlongitude, endlatitude, endlongitude',
            "error_type": "Location Error",
            "error_message" : f'This station has lat, long outside of the bight strata'
        })
        errs = [*errs, checkData(**trawl_args)]


    # ------- END Trawl Checks ------- #

    if grab is not None:
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
        checkData(grab[(grab['grabfail'].isin(lu_gf.grabfail.tolist())) & (grab['comments'].isnull())].tmp_row.tolist(), 'Comments', 'Undefined Error', 'error', f'A comment is required for that stationfail option. Please see: <a href=/{current_app.script_root}/scraper?action=help&layer=lu_grabfails target=_blank>GrabFail lookup</a>.', grab)
        grab_args.update({
            "badrows": grab[(grab['grabfail'].isin(lu_gf.grabfail.tolist())) & (grab['comments'].isnull())].tmp_row.tolist(),
            "badcolumn": 'Comments',
            "error_type": "Undefined Error",
            "error_message" : f'A comment is required for that stationfail option. Please see: <a href=/{current_app.script_root}/scraper?action=help&layer=lu_grabfails target=_blank>GrabFail lookup</a>.'
        })
        errs = [*errs, checkData(**grab_args)]
        
        # Check if grab stationid is in field_assignment_table
        merged = pd.merge(
            grab, 
            field_assignment_table.filter(items=['stationid','stratum','region']), 
            how='left', 
            on=['stationid'],
            indicator=True

        )
        bad_df = merged[merged['_merge'] == 'left_only']
        if len(bad_df) > 0:
            grab_args.update({
                "badrows": bad_df.tmp_row.tolist(),
                "badcolumn": 'stationid',
                "error_type": "Lookup Error",
                "error_message" : f'These stations are not in the field assignment table'
            })
            errs = [*errs, checkData(**grab_args)]
            strata_check = False
        else:
            strata_check = True

        if strata_check:
            # Check if grab stations are in strata
            print("# Check if grab stations are in strata")
            bad_df = check_strata_grab(grab, strata_lookup, field_assignment_table)
            print(bad_df)
            if len(bad_df) > 0:
                export_sdf_to_json(os.path.join(session['submission_dir'], "bad_grab.json"), bad_df)
                export_sdf_to_json(os.path.join(session['submission_dir'], "bight_strata.json"), strata[strata['region'].isin(bad_df['region'])])

            grab_args.update({
                "badrows": bad_df.tmp_row.tolist(),
                "badcolumn": 'latitude,longitude',
                "error_type": "Location Error",
                "error_message" : f'This station has lat, long outside of the bight strata'
            })
            errs = [*errs, checkData(**grab_args)]
        print("end grab CHECKS")

        ## end grab CHECKS ##


    return {'errors': errs, 'warnings': warnings}
