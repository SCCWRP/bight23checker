# Dont touch this file! This is intended to be a template for implementing new custom checks

from inspect import currentframe
from flask import current_app
import pandas as pd
from .functions import checkData, get_badrows

def discretewq(all_dfs):
    
    current_function_name = str(currentframe().f_code.co_name)
    
    # function should be named after the dataset in app.datasets in __init__.py
    assert current_function_name in current_app.datasets.keys(), \
        f"function {current_function_name} not found in current_app.datasets.keys() - naming convention not followed"

    expectedtables = set(current_app.datasets.get(current_function_name).get('tables'))
    assert expectedtables.issubset(set(all_dfs.keys())), \
        f"""In function {current_function_name} - {expectedtables - set(all_dfs.keys())} not found in keys of all_dfs ({','.join(all_dfs.keys())})"""

    # since often times checks are done by merging tables (Paul calls those logic checks)
    # we assign dataframes of all_dfs to variables and go from there
    # This is the convention that was followed in the old checker
    
    # This data type should only have tbl_example
    # example = all_dfs['tbl_example']

    
    watermeta=all_dfs=['tbl_waterquality_metadata']
    waterdata=all_dfs['tbl_waterquality_data']

    errs = []
    warnings = []

    # Alter this args dictionary as you add checks and use it for the checkData function
    # for errors that apply to multiple columns, separate them with commas
    
    args = {
        "dataframe": pd.DataFrame({}),
        "tablename":'',
        "badrows": [],
        "badcolumn": "",
        "error_type": "",
        "is_core_error": False,
        "error_message": ""
    }
    

    # Example of appending an error (same logic applies for a warning)
    # args.update({
    #   "badrows": get_badrows(df[df.temperature != 'asdf']),
    #   "badcolumn": "temperature",
    #   "error_type" : "Not asdf",
    #   "error_message" : "This is a helpful useful message for the user"
    # })
    # errs = [*errs, checkData(**args)]

    args.update({
        "dataframe": watermeta,
        "tablename": 'tbl_waterquality_metadata',
        "badrows":watermeta[(watermeta['depth_m'] < 0)].index.tolist(),
        "badcolumn": "depth_m",
        "error_type" : "Value out of range",
        "error_message" : "Your depth value must be larger than 0."
    })
    errs = [*errs, checkData(**args)]
    
    args.update({
        "dataframe": watermeta,
        "tablename": 'tbl_waterquality_metadata',
        "badrows":watermeta['samplecollectiontime'].apply(lambda x: pd.Timestamp(str(x)).strftime('%H:%M:%S') if not pd.isnull(x) else "00:00:00").index.tolist(),
        "badcolumn": "samplecollectiontime",
        "error_type" : "Value out of range",
        "error_message" : "Your time input is out of range."
    })
    errs = [*errs, checkData(**args)]
    
    return {'errors': errs, 'warnings': warnings}