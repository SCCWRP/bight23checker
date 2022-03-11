# Dont touch this file! This is intended to be a template for implementing new custom checks

from inspect import currentframe
from flask import current_app, g
import pandas as pd
from .functions import checkData, get_badrows
from .yeahbuoy import yeahbuoy

def logger(all_dfs):
    
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

    loggermeta =  all_dfs['tbl_wq_logger_metadata']
    loggerm =     all_dfs['tbl_logger_mdot_data']
    loggerc =     all_dfs['tbl_logger_ctd_data']
    loggertroll = all_dfs['tbl_logger_troll_data']
    loggertid =   all_dfs['tbl_logger_tidbit_data']
    loggerother = all_dfs['tbl_logger_other_data']

   
    errs = []
    warnings = []

    # Alter this args dictionary as you add checks and use it for the checkData function
    # for errors that apply to multiple columns, separate them with commas
    args = {
        "dataframe": pd.DataFrame({}),
        "tablename": '',
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
    
    # Before the Barometric/Buoy routine
    nonrequired = {
        'tbl_logger_mdot_data': loggerm,
        'tbl_logger_ctd_data': loggerc,
        'tbl_logger_troll_data': loggertroll,
        'tbl_logger_tidbit_data': loggertid,
        'tbl_logger_other_data': loggerother
    }

    # first check if they have all required data
    print("# first check if they have all required data")
    if all([df.empty for df in nonrequired.values()]):
        for k, df in nonrequired.items():
            # add a row to the empty dataframe
            df = pd.DataFrame(eval( "{}".format({col: '' for col in df.columns}) ) , index = [0])
            
            args.update({
                "dataframe": df,
                "tablename": k,
                "badrows":[0],
                "badcolumn": ",".join([col for col in df.columns]),
                "error_type" : "Missing required data",
                "error_message" : f"Data must be provided for one of {', '.join(tbl for tbl in nonrequired.keys())}"
            })
            errs = [*errs, checkData(**args)]

            # return here if they dont have all the required data
            return {'errors': errs, 'warnings': warnings}

    else:
        for df in nonrequired.values():
            df = yeahbuoy(df) if not df.empty else df
    print("Done checking if they have all required data")
    
    print("Begin minidot data checks...")
    args.update({
        "dataframe": loggerm,
        "tablename": "tbl_logger_mdot_data",
        "badrows":loggerm[(loggerm['h2otemp_c'] > 100) | ((loggerm['h2otemp_c']!=-88) & (loggerm['h2otemp_c'] < 0))].index.tolist(),
        "badcolumn": "h2otemp_c",
        "error_type" : "Value out of range",
        "error_message" : "Your h2otemp_c is out of range. Value should not exceed 100 degrees C."
    })
    errs = [*errs, checkData(**args)]
    print("check ran - logger_mdot_data - h2otemp_c")

    args.update({
        "dataframe": loggerm,
        "tablename": "tbl_logger_mdot_data",
        "badrows":loggerm[(loggerm['do_percent'] < 0) & (loggerm['do_percent']!=-88)].index.tolist(),
        "badcolumn": "do_percent",
        "error_type" : "Value out of range",
        "error_message" : "Your do_percent is negative. Value must be nonnegative and at least 0."
    })
    errs = [*errs, checkData(**args)]
    print("check ran - logger_mdot_data - do_percent")

    # Check: issue warning do_percent > 110 # Jan asked for this to be a warning. 4 March 2022
    args.update({
        "dataframe": loggerm,
        "tablename": "tbl_logger_mdot_data",
        "badrows":loggerm[(loggerm['do_percent'] > 110)].index.tolist(),
        "badcolumn": "do_percent",
        "error_type" : "Value out of range",
        "error_message" : "Your do_percent is greater than 110. This is an unexpected value, but will be accepted."
    })
    warnings = [*warnings, checkData(**args)]
    print("check ran - logger_mdot_data - do_percent")

    args.update({
        "dataframe": loggerm,
        "tablename": "tbl_logger_mdot_data",
        "badrows":loggerm[(loggerm['do_mgl'] > 60) | ((loggerm['do_mgl']!=-88) & (loggerm['do_mgl'] < 0))].index.tolist(),
        "badcolumn": "do_mgl",
        "error_type" : "Value out of range",
        "error_message" : "Your do_mql value is out of range. Value should not exceed 60."
    })
    errs = [*errs, checkData(**args)]
    print("check ran - logger_mdot_data - do_mgl")

    # Check: qvalue range increased from 1 to 1.1 - approved by Jan 4 March 2022
    args.update({
        "dataframe": loggerm,
        "tablename": "tbl_logger_mdot_data",
        "badrows":loggerm[(loggerm['qvalue'] > 1.1) | ((loggerm['qvalue']!=-88) & (loggerm['qvalue'] < 0))].index.tolist(),
        "badcolumn": "qvalue",
        "error_type" : "Value out of range",
        "error_message" : "Your qvalue is out of range. Must be less than 1.1."
    })
    errs = [*errs, checkData(**args)]
    print("check ran - logger_mdot_data - qvalue")
    print("...End minidot data checks.")

    print("Begin CTD data checks...")
    args.update({
        "dataframe": loggerc,
        "tablename": "tbl_logger_ctd_data",
        "badrows":loggerc[((loggerc['conductivity_sm'] < 0) & (loggerc['conductivity_sm'] != -88)) | (loggerc['conductivity_sm'] > 10)].index.tolist(),
        "badcolumn": "conductivity_sm",
        "error_type" : "Value out of range",
        "error_message" : "Your conductivity_sm value is out of range. Value must be within 0-10. If no value to provide, enter -88."
    })
    errs = [*errs, checkData(**args)]
    print("check ran - logger_ctd_data - conductivity_sm")

    args.update({
        "dataframe": loggerc,
        "tablename": "tbl_logger_ctd_data",
        "badrows":loggerc[(loggerc['h2otemp_c'] > 100) | ((loggerc['h2otemp_c'] != -88) & (loggerc['h2otemp_c'] < 0))].index.tolist(),
        "badcolumn": "h2otemp_c",
        "error_type" : "Value out of range",
        "error_message" : "Your h2otemp_c value is out of range. Value should not exceed 100 degrees C. If no value to provide, enter -88."
    })
    errs = [*errs, checkData(**args)]
    print("check ran - logger_ctd_data - h2otemp_c")

    args.update({
        "dataframe": loggerc,
        "tablename": "tbl_logger_ctd_data",
        "badrows":loggerc[(loggerc['salinity_ppt'] < 0) & (loggerc['salinity_ppt'] != -88)].index.tolist(),
        "badcolumn": "salinity_ppt",
        "error_type" : "Negative value",
        "error_message" : "Your salinity_ppt value is less than 0. Value should be nonnegative. If no value to provide, enter -88."
    })
    errs = [*errs, checkData(**args)]
    print("check ran - logger_ctd_data - salinity_ppt")
    print("...End CTD data checks.")

    print("Begin Troll data checks...")
    args.update({
        "dataframe": loggertroll,
        "tablename": "tbl_logger_troll_data",
        "badrows":loggertroll[(loggertroll['h2otemp_c'] > 100) | ((loggertroll['h2otemp_c'] != -88) & (loggertroll['h2otemp_c'] < 0))].index.tolist(),
        "badcolumn": "h2otemp_c",
        "error_type" : "Value out of range",
        "error_message" : "Your h2otemp_c value is out of range. Value should not exceed 100 degrees C. If no value to provide, enter -88."
    })
    errs = [*errs, checkData(**args)]
    print("check ran - logger_troll_data - h2otemp_c")
    print("...End Troll data checks.")

    print("Begin Tidbit data checks...")
    args.update({
        "dataframe": loggertid,
        "tablename": "tbl_logger_tidbit_data",
        "badrows":loggertid[(loggertid['h2otemp_c'] > 100) | ((loggertid['h2otemp_c'] != -88) & (loggertid['h2otemp_c'] < 0))].index.tolist(),
        "badcolumn": "h2otemp_c",
        "error_type" : "Value out of range",
        "error_message" : "Your h2otemp_c value is out of range. Value should not exceed 100 degrees C. If no value to provide, enter -88."
    })
    errs = [*errs, checkData(**args)]
    print("check ran - logger_tidbit_data - h2otemp_c")
    print("...End Tidbit data checks.")

    print("Begin Other data checks...")
    args.update({
        "dataframe": loggerother,
        "tablename": "tbl_logger_other_data",
        "badrows":loggerother[(loggerother['h2otemp_c'] > 100) | ((loggerother['h2otemp_c'] != -88) & (loggerother['h2otemp_c'] < 0))].index.tolist(),
        "badcolumn": "h2otemp_c",
        "error_type" : "Value out of range",
        "error_message" : "Your h2otemp_c value is out of range. Value should be within 0 to 100 degrees C. If no value to provide, enter -88."
    })
    errs = [*errs, checkData(**args)]
    print("check ran - logger_other_data - h2otemp_c")

    args.update({
        "dataframe": loggerother,
        "tablename": "tbl_logger_other_data",
        "badrows":loggerother[(loggerother['ph'] < 1) | (loggerother['ph'] > 14)].index.tolist(),
        "badcolumn": "ph",
        "error_type" : "Value out of range",
        "error_message" : "pH value is out of range. Value should be between 1 and 14. If no value to provide, enter -88."
    })
    errs = [*errs, checkData(**args)]
    print("check ran - logger_other_data - pH")

    args.update({
        "dataframe": loggerother,
        "tablename": "tbl_logger_ctd_data",
        "badrows":loggerother[((loggerother['conductivity_sm'] < 0) & (loggerother['conductivity_sm'] != -88)) | (loggerother['conductivity_sm'] > 10)].index.tolist(),
        "badcolumn": "conductivity_sm",
        "error_type" : "Value out of range",
        "error_message" : "Your conductivity_sm value is out of range. Value must be within 0-10. If no value to provide, enter -88."
    })
    errs = [*errs, checkData(**args)]
    print("check ran - logger_other_data - conductivity_sm")

    args.update({
        "dataframe": loggerother,
        "tablename": "tbl_logger_other_data",
        "badrows":loggerother[((loggerother['turbitidy_ntu'] < 0) & (loggerother['turbitidy_ntu'] != -88)) | (loggerother['turbitidy_ntu'] > 3000)].index.tolist(),
        "badcolumn": "turbitidy_ntu",
        "error_type" : "Value out of range",
        "error_message" : "Turbidity_NTU value is out of range. Value should be within 0-3000. If no value to provide, enter -88."
    })
    errs = [*errs, checkData(**args)]
    print("check ran - logger_other_data - turbidity_ntu")

    args.update({
        "dataframe": loggerother,
        "tablename": "tbl_logger_other_data",
        "badrows":loggerother[(loggerother['do_mgl'] > 60) | ((loggerother['do_mgl']!=-88) & (loggerother['do_mgl'] < 0))].index.tolist(),
        "badcolumn": "do_mgl",
        "error_type" : "Value out of range",
        "error_message" : "DO_mgL value is out of range. Value should be within 0-60. If no value to provide, enter -88."
    })
    errs = [*errs, checkData(**args)]
    print("check ran - logger_other_data - do_mgl")

    args.update({
        "dataframe": loggerother,
        "tablename": "tbl_logger_other_data",
        "badrows":loggerother[(loggerother['do_percent'] < 0) & (loggerother['do_percent']!=-88)].index.tolist(),
        "badcolumn": "do_percent",
        "error_type" : "Value out of range",
        "error_message" : "DO_percent is negative. Value must be nonnegative and at least 0."
    })
    errs = [*errs, checkData(**args)]
    print("check ran - logger_other_data - do_percent")

    args.update({
        "dataframe": loggerother,
        "tablename": "tbl_logger_other_data",
        "badrows":loggerother[(loggerother['do_percent'] > 110)].index.tolist(),
        "badcolumn": "do_percent",
        "error_type" : "Value out of range",
        "error_message" : "DO_percent is greater than 110. This is an unexpected value, but will be accepted."
    })
    warnings = [*warnings, checkData(**args)]
    print("check ran - logger_other_data - do_percent")

    args.update({
        "dataframe": loggerother,
        "tablename": "tbl_logger_other_data",
        "badrows":loggerother[(loggerother['orp_mv'] < -999) | (loggerother['orp_mv'] > 999)].index.tolist(),
        "badcolumn": "orp_mv",
        "error_type" : "Value out of range",
        "error_message" : "ORP_mV is out of range. Value must be within -999 to 999."
    })
    errs = [*errs, checkData(**args)]
    print("check ran - logger_other_data - orp_mv")

    args.update({
        "dataframe": loggerother,
        "tablename": "tbl_logger_other_data",
        "badrows":loggerother[(loggerother['salinity_ppt'] < 0) & (loggerother['salinity_ppt'] != -88)].index.tolist(),
        "badcolumn": "salinity_ppt",
        "error_type" : "Value out of range",
        "error_message" : "Salinity_ppt entered is negative. Value must be at least 0. If no value to provide, enter -88."
    })
    errs = [*errs, checkData(**args)]
    print("check ran - logger_other_data - salinity_ppt")

    print("...End Other data checks.")
    
    return {'errors': errs, 'warnings': warnings}

