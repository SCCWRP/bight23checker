# Dont touch this file! This is intended to be a template for implementing new custom checks

from inspect import currentframe
from flask import current_app
from .functions import checkData
import re
import pandas as pd
from datetime import timedelta


def ocean_acidification(all_dfs):
    
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

    ctd = all_dfs['tbl_oactd']
    bottle = all_dfs['tbl_oabottle']

    ctd['tmp_row'] = ctd.index
    bottle['tmp_row'] = bottle.index

    ctd_args = {
        "dataframe": ctd,
        "tablename": 'tbl_oactd',
        "badrows": [],
        "badcolumn": "",
        "error_type": "",
        "is_core_error": False,
        "error_message": ""
    }

    bottle_args = {
        "dataframe": bottle,
        "tablename": 'tbl_oabottle',
        "badrows": [],
        "badcolumn": "",
        "error_type": "",
        "is_core_error": False,
        "error_message": ""
    }

    ##################################
    # FORCE TIME DATATYPE AND FORMAT #
    ##################################
    '''
    We need to force the time datatype/format because the validator in Core does not differentiate between strings of times and times. 
    Character varying fields should only be allowing strings, but for some reason if the submitted data has time objects, it 
    gets through the validator. Forcing the time datatype and format gives us absolute certainty to perform checks for these
    particular fields. 
    '''
    print("checking the date and time formats")

    # We had an issue where there was whitespace in front of the time, causing an error which a user could not understand
    # Need to confirm with Karen that this is OK, but for ctd and bottle dataframes, we will strip whitespace
    ctd.sampletime = ctd.sampletime.apply(lambda x: str(x).strip())
    bottle.sampletime = bottle.sampletime.apply(lambda x: str(x).strip())
    
    timepattern = re.compile(r'[0-9]{1,2}:[0-5][0-9]:[0-5][0-9]$')

    # check time format
    badrows = ctd[ctd.sampletime.apply(lambda x: not bool(re.match(timepattern, x)) if not pd.isnull(x) else False)].tmp_row.tolist()
    ctd_args.update({
        "badrows": badrows,
        "badcolumn": "sampletime",
        "error_type": "Formatting Error",
        "error_message": "The sampletime here doesnt match the 24 hour clock format HH:MM:SS"
    })
    errs.append(checkData(**ctd_args))
    

    badrows = bottle[bottle.sampletime.apply(lambda x: not bool(re.match(timepattern, x)) if not pd.isnull(x) else False)].tmp_row.tolist()
    bottle_args.update({
        "badrows": badrows,
        "badcolumn": "sampletime",
        "error_type": "Formatting Error",
        "error_message": "The sampletime here doesnt match the 24 hour clock format HH:MM:SS"
    })
    errs.append(checkData(**bottle_args))

    badrows = ctd[ctd.sampletime.apply(lambda x: bool(int(re.match(timepattern, x).groups()[0]) > 23))].tmp_row.tolist()
    ctd_args.update({
        "badrows": badrows,
        "badcolumn": "sampletime",
        "error_type": "Formatting Error",
        "error_message": "The sampletime here doesnt match the 24 hour clock format HH:MM:SS"
    })
    errs.append(checkData(**ctd_args))
    
    badrows = bottle[bottle.sampletime.apply(lambda x: bool(int(re.match(timepattern, x).groups()[0]) > 23))].tmp_row.tolist()
    bottle_args.update({
        "badrows": badrows,
        "badcolumn": "sampletime",
        "error_type": "Formatting Error",
        "error_message": "The sampletime here doesnt match the 24 hour clock format HH:MM:SS"
    })
    errs.append(checkData(**bottle_args))
    
    
    # clean up the errors list
    errs = [e for e in errs if len(e) > 0]

    if len(errs) == 0:

        # Check to see if there is only one Season and agency per submission.
        if len(ctd.agency.unique()) != 1:
            errs = [*errs, checkData('tbl_oactd', ctd.tmp_row.tolist(), "Agency", "Undefined Error", "There may be only one Agency per submission.")]
        if len(ctd.season.unique()) != 1:
            errs = [*errs, checkData('tbl_oactd', ctd.tmp_row.tolist(), "Season", "Undefined Error", "There may be only one Season per submission.")]

        if len(bottle.agency.unique()) != 1:
            errs = [*errs, checkData('tbl_oabottle', bottle.tmp_row.tolist(), "Agency", "Undefined Error", "There may be only one Agency per submission.")]
        if len(bottle.season.unique()) != 1:
            errs = [*errs, checkData('tbl_oabottle', bottle.tmp_row.tolist(), "Season", "Undefined Error", "There may be only one Season per submission.")]


    # clean up the errors list
    errs = [e for e in errs if len(e) > 0]

    print("OA Logic Check")
    print("Bottle record must have a match in CTD - not necessarily the other way around though.")
    if len(errs) == 0:
        matching_cols = ['season','agency','sampledate','station','fieldrep','labrep']
        badrows = bottle[~bottle[matching_cols].isin(ctd[matching_cols].to_dict(orient='list')).all(axis=1)].tmp_row.tolist()
        errs = [*errs, checkData('tbl_oabottle', badrows, ",".join(matching_cols), "Undefined Error",  "Each Bottle record must have a corresponding CTD record. Records are matched on Season, Agency, SampleDate, Station, FieldRep and LabRep.")]


    # clean up the errors list
    errs = [e for e in errs if len(e) > 0]

    # previous checks must pass so that the app does not critical
    if len(errs) == 0:
        
        # Depth agreement - Allowable difference in sampling depths between CTD casts and Bottle samples
        print('# Depth agreement - Allowable difference in sampling depths between CTD casts and Bottle samples')
        
        '''
        Warning: Not enough information provided, so some information will be assumed. Be sure to change any wrong assumptions
                before final implementation - Jordan

        Assumptions: 1. Records will be matched on Season, Agency, SampleDate, Station, Depth, FieldRep, and LabRep
                    2. Allowable Time difference will be set to 4 hours
                    3. Arbitrarily chose to output errors to Bottle dataframe
        '''
        
        # subset dataframes to only look at pertinent fields
        sub_ctd = ctd[['season', 'agency', 'sampledate', 'station', 'depth', 'fieldrep', 'labrep']]
        sub_bottle = bottle[['season', 'agency', 'sampledate', 'station', 'depth', 'fieldrep', 'labrep','tmp_row']]
        # merge records on assumed fields. (check assumption 1 above)
        #depths = sub_ctd.merge(sub_bottle, on = ['season', 'agency', 'sampledate', 'station', 'fieldrep', 'labrep'], how = 'inner')
        # compare depths 

        # Below code is from Lily. Moved into production 7/9/2019 - Robert
        # I will try to make it work with existing code since it seems that records are matched on more thab simply stationcode and date. - Robert 7/9/2019
        sub_bottle[['plusdepth']] = sub_bottle[['depth']]+1
        sub_bottle[['minusdepth']] = sub_bottle[['depth']]-1

        print("merging bottle and ctd")
        
        depths = sub_ctd.merge(sub_bottle, on = ['season', 'agency', 'sampledate', 'station', 'fieldrep', 'labrep'], how = 'inner')
        #result = df_bottle.merge(df_ctd, on=['sampledate','station'],how='inner')
        
        print("renaming columns")
        depths.rename(columns={'depth_x':'depth_ctd', 'depth_y':'depth_bottle'}, inplace = True)

        #result = result.drop_duplicates(subset=['Station','Depth_bottle','plusdepth'], keep='first')

        print("checking to see if the ctd depth is within one of bottle depth")
        depths['rangedepth'] = depths['depth_ctd'].between(depths['minusdepth'], depths['plusdepth'], inclusive=True)\

        print("grouping")
        depths = depths.groupby(['season','agency','sampledate','station','fieldrep','labrep','depth_bottle'])['rangedepth'].apply(list).reset_index(name='boolean_list')

        print("checking if the depth was within the range")
        depths['depth-inside-range'] = depths['boolean_list'].apply(lambda x: sum(x)) > 0

        print("merging")
        depths = sub_bottle.merge(depths, left_on = ['season','agency','sampledate','station','fieldrep','labrep','depth'], right_on = ['season','agency','sampledate','station','fieldrep','labrep','depth_bottle'])

        badrows = depths[depths['depth-inside-range'] == False].tmp_row.tolist()
        
        
        errs.append(checkData('tbl_oabottle', badrows, "Depth", "Undefined Error", "Each record in bottle must have a matching record in ctd such that the difference in depth is not more than one foot."))
        

        # Time agreement - Allowable difference in the sampling time between CTD casts and Bottle samples
        '''
        Warning: Not enough information provided, so some information will be assumed. Be sure to change any wrong assumptions
                before final implementation - Jordan

        Assumptions: 1. Records will be matched on Season, Agency, SampleDate, Station, FieldRep, and LabRep
                    2. Allowable Time difference will be set to 4 hours
                    3. Arbitrarily chose to output errors to Bottle dataframe
        '''
        # subset dataframes to only look at pertinent fields
        sub_ctd = ctd[['season', 'agency', 'sampledate', 'station', 'fieldrep', 'labrep','sampletime']]
        sub_bottle = bottle[['season', 'agency', 'sampledate', 'station', 'fieldrep', 'labrep', 'sampletime']]
        # merge records on assumed fields. (check assumption 1 above)
        times = sub_ctd.merge(sub_bottle, on = ['season', 'agency', 'sampledate', 'station', 'fieldrep', 'labrep'], how = 'inner')
        # convert string of times into datetimes
        #times.sampletime_x = pd.to_datetime(times.sampletime_x)
        #times.sampletime_y = pd.to_datetime(times.sampletime_y)
    
        # convert string of times into times
        times.sampletime_x = times.sampletime_x.astype(str).apply(lambda x: pd.Timestamp(x))
        times.sampletime_y = times.sampletime_y.astype(str).apply(lambda x: pd.Timestamp(x))
        
        # check that sampletimes are within allowable time difference. (check assumption 2 above)        
        invalids = times[(times.sampletime_x - times.sampletime_y) > timedelta(hours = 4)]
        print("invalids")
        print(invalids)
        errs.append(checkData('tbl_oabottle', bottle.merge(invalids, on = ['season', 'agency', 'sampledate', 'station', 'fieldrep', 'labrep'], how = 'inner').tmp_row.unique().tolist(),'SampleTime','Logic Error','CTD SampleTime and Bottle SampleTime are outside allowable time difference.'))

        #######################
        # CTD EXTENDED CHECKS #
        #######################
        
    
        # Check Order of Field Replicates
        '''
        Assumptions: 1. For a given station, the field rep 2s must come after the field rep 1s
                    2. A single time value is associated to a single fieldrep (e.g. all records where fieldrep = 1, time is fixed)
        '''
        sub_ctd = ctd[['station','fieldrep','sampledate','sampletime']]
        sub_ctd.drop_duplicates(inplace=True)
        print("Here is sub_ctd.sampledate")
        print(sub_ctd.sampledate)
        sub_ctd.sampledate = sub_ctd.sampledate.apply(lambda x: x.strftime("%m/%d/%Y") if isinstance(x, datetime.datetime) else pd.Timestamp(x).date().strftime("%m/%d/%Y"))

        sub_ctd['datetime'] = pd.to_datetime(sub_ctd['sampledate'] + ' ' + sub_ctd['sampletime'])
        sub_ctd.drop(['sampledate','sampletime'], axis = 1, inplace = True)
        sub_ctd.reset_index(drop=True,inplace=True)
        print(sub_ctd)

        # initial replicate set
        init_rep = sub_ctd[sub_ctd.fieldrep == 1]
        # duplicate replicate set
        dupe_rep = sub_ctd[sub_ctd.fieldrep == 2]
        # merge on station
        dupes = init_rep.merge(dupe_rep, on = ['station'], how = 'inner')
        # check order of replicates
        invalid_stations = dupes[(dupes.datetime_y - dupes.datetime_x).dt.total_seconds() < 0].station.tolist()
        print("Now checking for nonsequential replicates")
        print(sub_ctd[sub_ctd.station.isin(invalid_stations)].drop_duplicates())
        errs.append(checkData('tbl_oactd', ctd[ctd.station.isin(invalid_stations)].drop_duplicates(subset = ['station','agency','fieldrep','sampletime']).tmp_row.tolist(),'SampleTime','Undefined Error', 'For a given station, replicate 2 records should only occur after replicate 1 records.'))


        # Range checks on Depth, Temperature, Salinity, Density, and pH
        '''
        Assumptions: 1. depth must be between 0 and 75 (no units provided)
                    2. temperature must be between 0 and 20 (no units provided)
                    3. salinity must be between 33.0 and 33.5 (no units provided)
                    4. density must be between 20 and 30 (no units provided)
                    5. pH must be between 7.5 and 8.2
                    6. all ranges are inclusive (i.e. boundary values included)
        '''
        
        # NOTE Do these range checks apply to bottle???? - Robert - 7/15/2019

        print("Now checking ranges on depth, temperature, salinity, density, and pH")
        # depth check
        print(ctd[(ctd.depth < 0)|(ctd.depth > 200)][['season','agency','sampledate','sampletime','station','depth']])
        errs.append(checkData('tbl_oactd', ctd[(ctd.depth < 0)|(ctd.depth > 200)].tmp_row.tolist(),'Depth','Range Warning', 'Depth value is outside of what would be considered a normal range of values, (0 to 200)'))
        # temperature check
        print(ctd[(ctd.temperature < 0)|(ctd.temperature > 20)][['season','agency','sampledate','sampletime','station','temperature']])
        errs.append(checkData('tbl_oactd', ctd[(ctd.temperature < 0)|(ctd.temperature > 20)].tmp_row.tolist(),'Temperature','Range Warning', 'Temperature value outside of what would be considered a normal range (e.g. 0-15).'))
        # salinity check
        print(ctd[(ctd.salinity < 33.0)|(ctd.salinity > 33.7)][['season','agency','sampledate','sampletime','station','salinity']])
        errs.append(checkData('tbl_oactd', ctd[(ctd.salinity < 33.0)|(ctd.salinity > 33.7)].tmp_row.tolist(),'Salinity','Range Warning', 'Salinity value outside of what would be condsidered a normal range (e.g. 33.2-33.5).'))
        # density check
        print(ctd[(ctd.density < 20)|(ctd.density > 30)][['season','agency','sampledate','sampletime','station','density']])
        errs.append(checkData('tbl_oactd', ctd[(ctd.density < 20)|(ctd.density > 30)].tmp_row.tolist(),'Density','Range Warning', 'Density value outside of what would be considered a normal range (e.g. 20-30).'))
        # pH check
        print(ctd[(ctd.ph < 7.5)|(ctd.ph > 8.2)][['season','agency','sampledate','sampletime','station','ph']])
        errs.append(checkData('tbl_oactd', ctd[(ctd.ph < 7.5)|(ctd.ph > 8.2)].tmp_row.tolist(), 'pH','Range Warning', 'pH value outside of what would be considered a normal range (e.g. 7.5-8.2).'))

        




    return {'errors': errs, 'warnings': warnings}
