# Dont touch this file! This is intended to be a template for implementing new custom checks

from inspect import currentframe
from flask import current_app, g
from datetime import timedelta
from .functions import checkData, checkLogic, sample_assignment_check
from .chem_functions_custom import *
import pandas as pd
import re

def chemistry_intercal(all_dfs):
    
    current_function_name = str(currentframe().f_code.co_name)
    
    # function should be named after the dataset in app.datasets in __init__.py
    assert current_function_name in current_app.datasets.keys(), \
        f"function {current_function_name} not found in current_app.datasets.keys() - naming convention not followed"

    expectedtables = set(current_app.datasets.get(current_function_name).get('tables'))
    if not expectedtables.issubset(set(all_dfs.keys())):
        print(f"""In function {current_function_name} - {expectedtables - set(all_dfs.keys())} not found in keys of all_dfs ({','.join(all_dfs.keys())})""")

    # DB Connection
    eng = g.eng

    # define errors and warnings list
    errs = []
    warnings = []

    results = all_dfs['tbl_chemresults_intercal']

    results['tmp_row'] = results.index

    # Tack on analyteclass
    results = results.merge(
        pd.read_sql("""SELECT analyte AS analytename, analyteclass FROM lu_analytes""", eng),
        on = 'analytename',
        how = 'inner'
    )

    # Calculate percent recovery
    results['percentrecovery'] = \
        results.apply(
            lambda x: 
            float(x.result)/float(x.truevalue)*100 
            if (('spike' in x.sampletype.lower())|('reference' in x.sampletype.lower())) & (x.truevalue != 0)
            else -88, 
            axis = 1
        )


    results_args = {
        "dataframe": results,
        "tablename": 'tbl_chemresults_intercal',
        "badrows": [],
        "badcolumn": "",
        "error_type": "",
        "is_core_error": False,
        "error_message": ""
    }

    
    # ----- CUSTOM CHECKS  ----- #
    print('# ----- CUSTOM CHECKS ----- #')

    
    # Check - TrueValue must be -88 for everything except CRM's and spikes (Matrix Spikes and blank spikes)
    # This is to be a Warning rather than an error
    # checked lu_sampletypes as of 11/18/2022 and these two cases will cover Reference Materials, blank spikes and matrix spikes
    # case = False makes the string match not case sensitive
    spike_mask = results.sampletype.str.contains('spike', case = False) | results.sampletype.str.contains('Reference', case = False)

    
    # badrows are deemed to be ones that are not in the "spike" or Reference category, but the TrueValue column is NOT a -88 (Warning)
    print('# badrows are deemed to be ones that are not in the "spike" or Reference category, but the TrueValue column is NOT a -88 (Warning)')
    badrows = results[(~spike_mask) & (results.truevalue != -88)].tmp_row.tolist()
    results_args.update({
        "badrows": badrows,
        "badcolumn": "TrueValue",
        "error_type": "Value Error",
        "error_message": "This row is not a 'spike' or a CRM, so the TrueValue should be -88"
    })
    warnings.append(checkData(**results_args))
    
    
    # badrows here could be considered as ones that ARE CRM's / spikes, but the TrueValue is missing (Warning)
    print('# badrows here could be considered as ones that ARE CRMs / spikes, but the TrueValue is missing (Warning)')
    badrows = results[(spike_mask) & (results.truevalue < 0)].tmp_row.tolist()
    results_args.update({
        "badrows": badrows,
        "badcolumn": "TrueValue",
        "error_type": "Value Error",
        "error_message": "This row is a 'spike' or a CRM, so the TrueValue should not be -88 (or any negative number)"
    })
    warnings.append(checkData(**results_args))

    # Check - Result column should be a positive number (except -88) for SampleType == 'Result' (Error)
    print("""# Check - Result column should be a positive number (except -88) for SampleType == 'Result' (Error)""")
    badrows = results[(results.sampletype == 'Result') & (results.result != -88) & (results.result <= 0)].tmp_row.tolist()
    results_args.update({
        "badrows": badrows,
        "badcolumn": "Result",
        "error_type": "Value Error",
        "error_message": "The Result column (for SampleType = 'Result') should be a positive number (unless it is -88)"
    })
    errs.append(checkData(**results_args))

    # Check - The MDL should never be greater than the RL (Error)
    print('# Check - The MDL should never be greater than the RL (Error)')
    results_args.update({
        "badrows": results[results.mdl > results.rl].tmp_row.tolist(),
        "badcolumn": "MDL",
        "error_type": "Value Error",
        "error_message": "The MDL should never be greater than the RL"
    })
    errs.append(checkData(**results_args))
    
    # Check - The MDL should not be equal to the RL (Warning)
    print('# Check - The MDL should not be equal to the RL (Warning)')
    results_args.update({
        "badrows": results[results.mdl == results.rl].tmp_row.tolist(),
        "badcolumn": "MDL",
        "error_type": "Value Error",
        "error_message": "The MDL should not be equal the RL in most cases"
    })
    warnings.append(checkData(**results_args))
    
    # Check - The MDL should never be a negative number (Error)
    print('# Check - The MDL should never be a negative number (Error)')
    results_args.update({
        "badrows": results[results.mdl < 0].tmp_row.tolist(),
        "badcolumn": "MDL",
        "error_type": "Value Error",
        "error_message": "The MDL should not be negative"
    })
    errs.append(checkData(**results_args))


    
    # Check - if the qualifier is "less than" or "below method detection limit" Then the result must be -88 (Error)
    print('# Check - if the qualifier is "less than" or "below method detection limit" Then the result must be -88 (Error)')
    results_args.update({
        "badrows": results[results.qualifier.isin(["less than", "below method detection limit"]) & (results.result.astype(float) != -88)].tmp_row.tolist(),
        "badcolumn": "Qualifier, Result",
        "error_type": "Value Error",
        "error_message": "If the Qualifier is 'less than' or 'below method detection limit' then the Result should be -88"
    })
    warnings.append(checkData(**results_args))

    # Check - if the qualifier is "estimated" or "below reporting level" then the result must be between the mdl and rl (inclusive) (Error)
    print('# Check - if the qualifier is "estimated" or "below reporting level" then the result must be between the mdl and rl (inclusive) (Error)')
    results_args.update({
        "badrows": results[
                (results.qualifier.isin(["estimated", "below reporting level"])) & ((results.result < results.mdl) | (results.result > results.rl))
            ].tmp_row.tolist(),
        "badcolumn": "Qualifier, Result",
        "error_type": "Value Error",
        "error_message": "If the Qualifier is 'estimated' or 'below reporting level' then the Result should be between the MDL and RL (inclusive)"
    })
    warnings.append(checkData(**results_args))
    
    # Check - if the qualifier is less than, below mdl, below reporting level, or estimated, but the result > rl, then the wrong qualifier was used
    print('# Check - if the qualifier is less than, below mdl, below reporting level, or estimated, but the result > rl, then the wrong qualifier was used')
    results_args.update({
        "badrows": results[
                (results.qualifier.isin(["estimated", "below reporting level", "below method detection limit", "estimated"])) 
                & (results.result > results.rl)
            ].tmp_row.tolist(),
        "badcolumn": "Qualifier",
        "error_type": "Value Error",
        "error_message": "if qualifier is 'less than', 'below method detection limit', 'below reporting level' or 'estimated', but the Result > RL, then the incorrect qualifier was used"
    })
    errs.append(checkData(**results_args))

    # Check - if the qualifier is "none" then the result must be greater than the RL (Error)
    print('# Check - if the qualifier is "none" then the result must be greater than the RL (Error)')
    results_args.update({
        "badrows": results[(results.qualifier == 'none') & (results.result < results.rl)].tmp_row.tolist(),
        "badcolumn": "Qualifier, Result",
        "error_type": "Value Error",
        "error_message": "if the qualifier is 'none' then the result must be greater than or equal to the RL (See the <a href=https://checker.sccwrp.org/bight23checker/scraper?action=help&layer=lu_chemqualifiercodes target=_blank>qualifier lookup list</a> for reference)"
    })
    errs.append(checkData(**results_args))

    # Check - Comment is required if the qualifier says "analyst error" "contaminated" or "interference" (Error)
    print('# Check - Comment is required if the qualifier says "analyst error" "contaminated" or "interference" (Error)')
    results_args.update({
        "badrows": results[(results.qualifier.isin(["analyst error","contaminated","interference"])) & (results.fillna('').comments == '')].tmp_row.tolist(),
        "badcolumn": "Comments",
        "error_type": "Value Error",
        "error_message": "We would like you to enter a comment if the qualifier says 'analyst error' 'contaminated' or 'interference'"
    })
    warnings.append(checkData(**results_args))
    
    # Check - We would like the submitter to contact us if the qualifier says "analyst error" (Warning)
    print('# Check - We would like the submitter to contact us if the qualifier says "analyst error" (Warning)')
    results_args.update({
        "badrows": results[results.qualifier == "analyst error"].tmp_row.tolist(),
        "badcolumn": "Qualifier",
        "error_type": "Value Error",
        "error_message": "We would like to be contacted concerning this record of data. Please contact bight23-im@sccwrp.org"
    })
    warnings.append(checkData(**results_args))

    # Check - True Value should not be Zero
    print('# Check - True Value should not be Zero')
    results_args.update({
        "badrows": results[results.truevalue == 0].tmp_row.tolist(),
        "badcolumn": "truevalue",
        "error_type": "Value Error",
        "error_message": "The TrueValue should never be zero. If the TrueValue is unknown, then please fill in the cell with -88"
    })
    errs.append(checkData(**results_args))

    
    return {'errors': errs, 'warnings': warnings}
