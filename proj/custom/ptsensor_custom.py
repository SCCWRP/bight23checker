# Dont touch this file! This is intended to be a template for implementing new custom checks

from inspect import currentframe
from flask import current_app, g
from .functions import checkData
import pandas as pd


def ptsensor(all_dfs):
    
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

    ptsensorresults = all_dfs['tbl_ptsensorresults']
    ptsensorresults = ptsensorresults.assign(tmp_row = ptsensorresults.index)

    ptsensorresults_args = {
        "dataframe": ptsensorresults,
        "tablename": 'tbl_ptsensorresults',
        "badrows": [],
        "badcolumn": "",
        "error_type": "",
        "is_core_error": False,
        "error_message": ""
    }

    ## LOGIC ##
    print("Starting PTSensor Logic Checks")
    #Jordan - Station occupation and trawl event data should be submitted before pressure temperature data. Check those tables to make sure the agency has submitted those first. [records are matched on StationID, SampleDate, Sampling Organization, and Trawl Number]
    print('Station occupation and trawl event data should be submitted before pressure temperature data. Check those tables to make sure the agency has submitted those first. [records are matched on StationID, SampleDate, Sampling Organization, and Trawl Number] ')

    # call database for trawl event data
    eng = g.eng
    ta_db = eng.execute("SELECT stationid,sampledate,samplingorganization,trawlnumber FROM tbl_trawlevent;")
    ta = pd.DataFrame(ta_db.fetchall())
    
    if len(ta) > 0: 
        ta.columns = ta_db.keys()

        # Series containing pertinent trawl assemblage and fish abundance/biomass records
        trawl_assemblage = zip(
            ta.stationid, 
            ta.sampledate,
            ta.samplingorganization,
            ta.trawlnumber
        )
        pt_data = pd.Series(
            zip(
                ptsensorresults.stationid,
                ptsensorresults.sampledate,
                ptsensorresults.samplingorganization,
                ptsensorresults.trawlnumber
            )
        )

        # Check To see if there is any data in fish abundance, not in trawl assemblage and vice versa
        print(ptsensorresults.loc[~pt_data.apply(lambda x: x in trawl_assemblage)])
        badrows = ptsensorresults.loc[
            ~pt_data.apply(
                lambda x: x in trawl_assemblage
            )
        ].tmp_row.tolist()
        ptsensorresults_args = {
            "dataframe": ptsensorresults,
            "tablename": 'tbl_ptsensorresults',
            "badrows": badrows,
            "badcolumn": "stationid,sampledate,samplingorganization,trawlnumber",
            "error_type": "Logic Error",
            "is_core_error": False,
            "error_message": "Each PTSensorResult record must have a corresponding Occupation and Trawl Event record. Records are matched on StationID, SampleDate, Sampling Organization, and Trawl Number."
        }        
        errs = [*errs, checkData(**ptsensorresults_args)]
    else:
        badrows = ptsensorresults.tmp_row.tolist()
        ptsensorresults_args = {
            "dataframe": ptsensorresults,
            "tablename": 'tbl_ptsensorresults',
            "badrows": badrows,
            "badcolumn": "stationid",
            "error_type": "Undefined Error",
            "is_core_error": False,
            "error_message": "Field data must be submitted before ptsensorresults data."
        }        
        errs = [*errs, checkData(**ptsensorresults_args)]    

    ## END LOGIC CHECKS ##

    ## CUSTOM CHECKS ##
    ## END CUSTOM CHECKS ##


    return {'errors': errs, 'warnings': warnings}
