# Dont touch this file! This is intended to be a template for implementing new custom checks

from inspect import currentframe
from flask import current_app, g
from datetime import timedelta
from .functions import checkData, checkLogic, sample_assignment_check
from .chem_functions_custom import *
import pandas as pd
import re

def chemistry(all_dfs):
    
    current_function_name = str(currentframe().f_code.co_name)
    
    # function should be named after the dataset in app.datasets in __init__.py
    assert current_function_name in current_app.datasets.keys(), \
        f"function {current_function_name} not found in current_app.datasets.keys() - naming convention not followed"

    expectedtables = set(current_app.datasets.get(current_function_name).get('tables'))
    assert expectedtables.issubset(set(all_dfs.keys())), \
        f"""In function {current_function_name} - {expectedtables - set(all_dfs.keys())} not found in keys of all_dfs ({','.join(all_dfs.keys())})"""

    # DB Connection
    eng = g.eng

    # define errors and warnings list
    errs = []
    warnings = []

    batch = all_dfs['tbl_chembatch']
    results = all_dfs['tbl_chemresults']

    batch['tmp_row'] = batch.index
    results['tmp_row'] = results.index

    # Tack on analyteclass
    results = results.merge(
        pd.read_sql("""SELECT analyte AS analytename, analyteclass FROM lu_analytes""", eng),
        on = 'analytename',
        how = 'inner'
    )

    # # Calculate percent recovery - moving this before Chemistry QA checks
    # # if truevalue is 0 - critical error: float division by zero BUG
    # results['percentrecovery'] = \
    #     results.apply(
    #         lambda x: 
    #         float(x.result)/float(x.truevalue)*100 if ('spike' in x.sampletype.lower())|('reference' in x.sampletype.lower()) else -88, 
    #         axis = 1
    #     )

    # Later on we will most likely need to manipulate the labsampleid field to be what we need it to be 
    results['sampleid'] = results.labsampleid

    batch_args = {
        "dataframe": batch,
        "tablename": 'tbl_chembatch',
        "badrows": [],
        "badcolumn": "",
        "error_type": "",
        "is_core_error": False,
        "error_message": ""
    }

    results_args = {
        "dataframe": results,
        "tablename": 'tbl_chemresults',
        "badrows": [],
        "badcolumn": "",
        "error_type": "",
        "is_core_error": False,
        "error_message": ""
    }

    # ----- LOGIC CHECKS ----- # 
    print('# ----- LOGIC CHECKS ----- # ')
    # Batch and Results must have matching records on Lab, PreparationBatchID and SampleID

    # check records that are in batch but not in results
    # checkLogic function is not being used since it marks incorrect rows on marked excel file return
    # Check for records in batch but not results
    badrows = batch[~batch[['lab','preparationbatchid']].isin(results[['lab','preparationbatchid']].to_dict(orient='list')).all(axis=1)].tmp_row.tolist()
    batch_args.update({
        "badrows": badrows,
        "badcolumn": "Lab, PreparationBatchID",
        "error_type": "Logic Error",
        "is_core_error": False,
        "error_message": "Each record in Chemistry Batch must have a matching record in Chemistry Results. Records are matched on Lab and PreparationID."
    })
    errs.append(checkData(**batch_args))

    # Check for records in results but not batch
    badrows = results[~results[['lab','preparationbatchid']].isin(batch[['lab','preparationbatchid']].to_dict(orient='list')).all(axis=1)].tmp_row.tolist()
    results_args.update({
        "badrows": badrows,
        "badcolumn": "Lab, PreparationBatchID",
        "error_type": "Logic Error",
        "is_core_error": False,
        "error_message": "Each record in Chemistry Results must have a matching record in Chemistry Batch. Records are matched on Lab and PreparationID."
    })
    errs.append(checkData(**results_args))


    # Check to see if GrainSize was submitted along with Sediment Results
    grain_analytes = pd.read_sql("SELECT analyte FROM lu_analytes WHERE analyteclass = 'GrainSize';", eng).analyte.tolist()
    grain_bool = results.analytename.isin(grain_analytes)

    # if there is a mixture of analyteclasses (GrainSize and non-GrainSize) the data should be flagged
    if not ((all(grain_bool)) or (all(~grain_bool))):
        n_grain = sum(grain_bool)
        n_nongrain = sum(~grain_bool)

        # If there are less grainsize records, flag them as being the bad rows. Otherwise flag the non grainsize rows
        results_args.update({
            "badrows": results[(grain_bool) if (n_grain < n_nongrain) else (~grain_bool)].tmp_row.tolist(),
            "badcolumn": "AnalyteName",
            "error_type": "Logic Error",
            "error_message": "You are attempting to submit grainsize analytes along with other sediment chemistry analytes. Sediment Chemistry Results must be submitted separately from Grainsize data"
        })
        errs.append(checkData(**results_args))
        
        # If they have mixed data, stop them here for the sake of time
        return {'errors': errs, 'warnings': warnings}

    # Sample Assignment check - make sure they were assigned the analyteclasses that they are submitting
    badrows = sample_assignment_check(eng = eng, df = results, parameter_column = 'analyteclass')
    
    results_args.update({
        "badrows": badrows,
        "badcolumn": "StationID,Lab,AnalyteName",
        "error_type": "Logic Error",
        "error_message": f"Your lab was not assigned to submit data for this analyteclass from this station (<a href=/{current_app.config.get('APP_SCRIPT_ROOT')}/scraper?action=help&layer=vw_sample_assignment&datatype=chemistry target=_blank>see sample assignments</a>)"
    })
    warnings.append(checkData(**results_args))

    # ----- END LOGIC CHECKS ----- # 
    print('# ----- END LOGIC CHECKS ----- # ')

        
    # ----- CUSTOM CHECKS - GRAINSIZE RESULTS ----- #
    if all(grain_bool):
        print('# ----- CUSTOM CHECKS - GRAINSIZE RESULTS ----- #')
        # Check - Units must be %
        results_args.update({
            "badrows": results[results.units != '%'].tmp_row.tolist(),
            "badcolumn": "Units",
            "error_type": "Value Error",
            "error_message": "For GrainSize data, units must be %"
        })
        errs.append(checkData(**results_args))
        
        # Check - for each grouping of stationid, fieldduplicate, labreplicate, the sum of the results should be between 99.8 and 100.2
        tmp = results.groupby(['stationid','fieldduplicate','labreplicate']).apply(lambda df: df.result.sum())
        if tmp.empty:
            return {'errors': errs, 'warnings': warnings}

        tmp = tmp.reset_index(name='resultsum')
        tmp = tmp[(tmp.resultsum < 99.8) | (tmp.resultsum > 100.2)]
        
        if tmp.empty:
            return {'errors': errs, 'warnings': warnings}

        checkdf = results.merge(tmp, on = ['stationid','fieldduplicate','labreplicate'], how = 'inner')
        checkdf = checkdf \
            .groupby(['stationid','fieldduplicate','labreplicate','resultsum']) \
            .apply(lambda df: df.tmp_row.tolist()) \
            .reset_index(name='badrows')

        tmp_argslist = checkdf.apply(
            lambda row: 
            {
                "badrows": row.badrows,
                "badcolumn": "Result",
                "error_type": "Value Error",
                "error_message": f"For this grouping of StationID: {row.stationid}, FieldDuplicate: {row.fieldduplicate}, and LabReplicate: {row.labreplicate}, the sum of the results was {row.resultsum}, which is outside of a range we would consider as normal (99.8 to 100.2)"
            },
            axis = 1
        ).values

        for argset in tmp_argslist:
            results_args.update(argset)
            errs.append(checkData(**results_args))
        
        print('# ----- END CUSTOM CHECKS - GRAINSIZE RESULTS ----- #')
        return {'errors': errs, 'warnings': warnings}
    # ----- END CUSTOM CHECKS - GRAINSIZE RESULTS ----- #
    

    # ----- CUSTOM CHECKS - SEDIMENT RESULTS ----- #
    print('# ----- CUSTOM CHECKS - SEDIMENT RESULTS ----- #')

    # Check for All required analytes per station (All or nothing)
    current_matrix = 'sediment' #sediment or tissue - affects query for required analytes
    # Check for all required analytes per station - if a station has a certain analyteclass
    req_anlts = pd.read_sql(f"SELECT analyte AS analytename, analyteclass FROM lu_analytes WHERE b23{current_matrix}='yes'", eng) \
        .groupby('analyteclass')['analytename'] \
        .apply(set) \
        .to_dict()
    
    chkdf = results.groupby(['stationid','analyteclass'])['analytename'].apply(set).reset_index()
    chkdf['missing_analytes'] = chkdf.apply(
        lambda row: ', '.join(list((req_anlts.get(row.analyteclass) if req_anlts.get(row.analyteclass) is not None else set()) - row.analytename)), axis = 1 
    )

    chkdf = chkdf[chkdf.missing_analytes != set()]
    if not chkdf.empty:
        chkdf = results.merge(chkdf[chkdf.missing_analytes != ''], how = 'inner', on = ['stationid','analyteclass'])
        chkdf = chkdf.groupby(['stationid','analyteclass','missing_analytes']).agg({'tmp_row': list}).reset_index()
        errs_args = chkdf.apply(
            lambda row:
            {
                "badrows": row.tmp_row,
                "badcolumn" : "stationid",
                "error_type": "missing_data",
                "error_message" : f"For the station {row.stationid}, you attempted to submit {row.analyteclass} but are missing some required analytes ({row.missing_analytes})"
            },
            axis = 1
        ).tolist()

        for argset in errs_args:
            results_args.update(argset)
            errs.append(checkData(**results_args))
    # End of checking all required analytes per station, if they attempted submission of an analyteclass
    # No partial submissions of analyteclasses

    # ------------------------- Begin chemistry base checks ----------------------------- #

    # Check - If the sampletype is "Lab blank" or "Blank spiked" then the matrix must be labwater or Ottawa sand
    results_args.update({
        "badrows": results[(results.sampletype.isin(["Lab blank","Blank spiked"])) & (~results.matrix.isin(["labwater","Ottawa sand"]))].tmp_row.tolist(),
        "badcolumn" : "matrix",
        "error_type": "Value error",
        "error_message" : "If the sampletype is Lab blank or Blank spiked, the only options for matrices would be 'labwater' or 'Ottawa sand'"
    })
    errs.append(checkData(**results_args))


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
        "error_message": "This row is not a Matrix spike, Blank spiked or a CRM Reference Material, so the TrueValue should be -88"
    })
    warnings.append(checkData(**results_args))
    
    # badrows here could be considered as ones that ARE CRM's / spikes, but the TrueValue is missing (Warning)
    print('# badrows here could be considered as ones that ARE CRMs / spikes, but the TrueValue is missing (Warning)')
    badrows = results[(spike_mask) & ((results.truevalue <= 0) | results.truevalue.isnull())].tmp_row.tolist()
    results_args.update({
        "badrows": badrows,
        "badcolumn": "TrueValue",
        "error_type": "Value Error",
        "error_message": "This row is a Matrix spike, Blank spiked, or a CRM Reference Material, so the TrueValue should not be -88 (or any negative number)"
    })
    warnings.append(checkData(**results_args))


    # Check - Result column should be a positive number (except -88) for all SampleTypes (Error)
    print("""# Check - Result column should be a positive number (except -88) for all SampleTypes (Error)""")
    badrows = results[(results.result != -88) & (results.result <= 0)].tmp_row.tolist()
    results_args.update({
        "badrows": badrows,
        "badcolumn": "Result",
        "error_type": "Value Error",
        "error_message": "The Result column for all SampleTypes should be a positive number (unless it is -88)"
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


    # Check - if result > RL then the qualifier cannot say "below reporting limit or "below method detection limit"
    print('# Check - if result > RL then the qualifier cannot say "below reporting limit or "below method detection limit"')
    results_args.update({
        "badrows": results[(results.result > results.rl) & (results.qualifier.isin(['below reporting limit','below method detection limit']))].tmp_row.tolist(),
        "badcolumn": "Qualifier",
        "error_type": "Value Error",
        "error_message": """if result > RL then the qualifier cannot say 'below reporting limit' or 'below method detection limit'"""
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
    errs.append(checkData(**results_args))

    # Check - if the qualifier is "estimated" or "below reporting level" then the result must be between the mdl and rl (inclusive) (Error)
    print('# Check - if the qualifier is "estimated" or "below reporting level" then the result must be between the mdl and rl (inclusive) (Error)')
    results_args.update({
        "badrows": results[
                ((results.qualifier.isin(["estimated", "below reporting level"])) & (results.sampletype != 'Lab blank'))
                & (
                    (results.result < results.mdl) | (results.result > results.rl)
                )
            ].tmp_row.tolist(),
        "badcolumn": "Qualifier, Result",
        "error_type": "Value Error",
        "error_message": "If the Qualifier is 'estimated' or 'below reporting level' then the Result should be between the MDL and RL (Inclusive)"
    })
    errs.append(checkData(**results_args))
    
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

    # Check - if the qualifier is "none" then the result must be greater than the RL (Error) Except lab blanks
    print('# Check - if the qualifier is "none" or "equal to" then the result must be greater than the RL (Error) Except lab blanks')
    results_args.update({
        "badrows": results[
            (
                (results.qualifier.isin(['none', 'equal to'])) & (results.sampletype != 'Lab blank')
            ) & 
            (results.result <= results.rl)
        ].tmp_row.tolist(),
        "badcolumn": "Qualifier, Result",
        "error_type": "Value Error",
        "error_message": "if the qualifier is 'none' or 'equal to' then the result must be greater than the RL"
    })
    errs.append(checkData(**results_args))

    # Check - Comment is required if the qualifier says "analyst error" "contaminated" or "interference" (Error)
    print('# Check - Comment is required if the qualifier says "analyst error" "contaminated" or "interference" (Error)')
    results_args.update({
        "badrows": results[(results.qualifier.isin(["analyst error","contaminated","interference"])) & (results.comments.fillna('').str.replace('\s*','', regex = True) == '')].tmp_row.tolist(),
        "badcolumn": "Comments",
        "error_type": "Value Error",
        "error_message": "Comment is required if the qualifier says 'analyst error' 'contaminated' or 'interference'"
    })
    errs.append(checkData(**results_args))

    
    # Check - We would like the submitter to contact us if the qualifier says "analyst error" (Warning)
    print('# Check - We would like the submitter to contact us if the qualifier says "analyst error" (Warning)')
    results_args.update({
        "badrows": results[results.qualifier == "analyst error"].tmp_row.tolist(),
        "badcolumn": "Qualifier",
        "error_type": "Value Error",
        "error_message": "We would like to be contacted concerning this record of data. Please contact b23-im@sccwrp.org"
    })
    warnings.append(checkData(**results_args))


    # ----------------------------------------------------------------------------------------------------------------------------------#
    # Check that each analysis batch has all the required sampletypes (All of them should have "Result" for obvious reasons) (Error)
    print('# Check that each analysis batch has all the required sampletypes (All of them should have "Result" for obvious reasons) (Error)')
    # Analyte classes: Inorganics, PAH, PCB, Chlorinated Hydrocarbons, Pyrethroid, PBDE, FIPRONIL, Lipids, TN, TOC
    # Required sampletypes by analyteclass:
    # Inorganics: Method blank, Reference Material, Matrix Spike, Blank Spike
    # PAH: Method blank, Reference Material, Matrix Spike
    # PCB, Chlorinated Hydrocarbons, PBDE: Method blank, Reference Material, Matrix Spike
    # Pyrethroid, FIPRONIL: Method blank, Matrix Spike
    # TN: Method blank
    # TOC: Method blank, Reference Material
    error_args = []
    
    required_sampletypes = {
        "Inorganics": ['Lab blank', 'Blank spiked', 'Result','Reference - ERA 540 Sed'],
        "PAH": ['Lab blank', 'Blank spiked', 'Matrix spike', 'Result', 'Reference - SRM 1944 Sed'],
        "PCB": ['Lab blank', 'Blank spiked', 'Matrix spike', 'Result', 'Reference - SRM 1944 Sed'],
        "Chlorinated Hydrocarbons": ['Lab blank', 'Blank spiked', 'Matrix spike', 'Result', 'Reference - SRM 1944 Sed'],
        "PBDE": ['Lab blank', 'Blank spiked', 'Matrix spike', 'Result', 'Reference - SRM 1944 Sed'],
        "Pyrethroid": ['Lab blank', 'Blank spiked', 'Matrix spike', 'Result'],
        "Neonicotinoids": ['Lab blank', 'Blank spiked', 'Matrix spike', 'Result'],
        "TIREWEAR": ['Lab blank', 'Blank spiked', 'Matrix spike', 'Result'],
        "TN" : ['Lab blank', 'Result'],
        "TOC" : ['Lab blank', 'Result', 'Reference - SRM 1944 Sed']
    }

    # anltclass = analyteclass
    # smpltyps = sampletypes
    # Just temp variables for this code block
    for anltclass, smpltyps in required_sampletypes.items():
        print("Check required sampletypes")
        error_args = [*error_args, *chk_required_sampletypes(results, smpltyps, anltclass)]
        
        print("Check for sample duplicates or matrix spike duplicates")
        if anltclass != 'Inorganics':
            print('Non-Inorganics')
            # For Inorganics, they can have either or, so the way we deal with inorganics must be different
            error_args = [*error_args, *check_dups(results, anltclass, 'Result')]
            error_args = [*error_args, *check_dups(results, anltclass, 'Matrix spike')]
        else:
            print('Inorganics')
            # Under the assumption of how we worded the error message. 
            # This will be to grab the batches that failed both the duplicate results and duplicate matrix spike check
            # Each batch has tp have at least one of those

            batch_regex = re.compile('The\s+AnalysisBatch\s+([^\s]*)')
            resargs = check_dups(results, anltclass, 'Result')
            spikeargs = check_dups(results, anltclass, 'Matrix spike')
            
            # If one of the lists is empty, then it means every single batch had a duplicate, 
            #  meaning the data is clean
            if ( (len(resargs) > 0) and (len(spikeargs) > 0) ):
                # make them into dataframes
                res = pd.DataFrame(resargs)
                res['batch'] = res.apply(
                    lambda row: 
                    re.search(batch_regex, row.error_message).groups()[0]
                    if re.search(batch_regex, row.error_message)
                    else '',
                    axis = 1
                )

                spike = pd.DataFrame(spikeargs)
                spike['batch'] = spike.apply(
                    lambda row: 
                    re.search(batch_regex, row.error_message).groups()[0]
                    if re.search(batch_regex, row.error_message)
                    else '',
                    axis = 1
                )

                argsdf = res.merge(spike[['batch']], on = 'batch', how = 'inner')

                if not argsdf.empty:
                    argsdf.error_message = argsdf.apply(
                        lambda row: f"""The AnalysisBatch {row.batch} needs either a duplicate sample result or a duplicate matrix spike""",
                        axis = 1
                    )
                    error_args = [*error_args, *argsdf.to_dict('records')]

    
    # NOTE needs to be updated
    requires_crm = ["Inorganics", "PAH", "PCB", "Chlorinated Hydrocarbons", "PBDE", "TOC", "TN"]
    error_args = [*error_args, *check_required_crm(results, requires_crm)]
    
    for argset in error_args:
        results_args.update(argset)
        errs.append(checkData(**results_args))

    
    
    # ----------------------------------------------------------------------------------------------------------------------------------#


    # Check - For Inorganics, units must be in ug/g dw (for Reference Materials, mg/kg dw is ok too) (Error)
    print('# Check - For Inorganics, units must be in ug/g dw (for Reference Materials, mg/kg dw is ok too) (Error)')
    units_metals_mask = (
        ((~results.sampletype.str.contains('Reference', case = False)) & (results.analyteclass == 'Inorganics')) 
        & (results.units != 'ug/g dw')
    ) 
    units_metals_crm_mask = (
        ((results.sampletype.str.contains('Reference', case = False)) & (results.analyteclass == 'Inorganics')) 
        & (~results.units.isin(['mg/kg dw','ug/g dw']))
    )
    results_args.update({
        "badrows": results[units_metals_mask | units_metals_crm_mask].tmp_row.tolist(),
        "badcolumn": "Units",
        "error_type": "Value Error",
        "error_message": "For Inorganics, units must be in ug/g dw (for Reference Materials, mg/kg dw is ok too)"
    })
    errs.append(checkData(**results_args))

    # Check - For sampletype Lab blank, if Result is less than MDL, it must be -88
    print('# Check - For sampletype Lab blank, if Result is less than MDL, it must be -88')
    # mb_mask = Lab blank mask (lab blank is also called method blank)
    print('# mb_mask = Lab blank mask')
    mb_mask = (results.sampletype == 'Lab blank') 
    results_args.update({
        "badrows": results[mb_mask & ((results.result < results.mdl) & (results.result != -88))].tmp_row.tolist(),
        "badcolumn": "Result",
        "error_type": "Value Error",
        "error_message": "For Lab blank sampletypes, if Result is less than MDL, it must be -88"
    })
    errs.append(checkData(**results_args))

    # Check - If SampleType=Lab blank and Result=-88, then qualifier must be below MDL or none.
    print('# Check - If SampleType=Lab blank and Result=-88, then qualifier must be below MDL or none.')
    results_args.update({
        "badrows": results[(mb_mask & (results.result == -88)) & (~results.qualifier.isin(['below method detection limit','none'])) ].tmp_row.tolist(),
        "badcolumn": "Qualifier",
        "error_type": "Value Error",
        "error_message": "If SampleType=Method blank and Result=-88, then qualifier must be 'below method detection limit' or 'none'"
    })
    errs.append(checkData(**results_args))

    # Check - True Value should not be Zero
    print('# Check - True Value should not be Zero')
    results_args.update({
        "badrows": results[results.truevalue == 0].tmp_row.tolist(),
        "badcolumn": "truevalue",
        "error_type": "Value Error",
        "error_message": "The TrueValue should never be zero. If the TrueValue is unknown, then please fill in the cell with -88"
    })
    errs.append(checkData(**results_args))


    # ---------------------------------------------------------------------------------------------------------------------------------#
    # ---------------------------------------------------------------------------------------------------------------------------------#
    print('# ---------------------------------------------------------------------------------------------------------------------------------#')
    # Check - Holding times for AnalyteClasses: 
    print('# Check - Holding times for AnalyteClasses: ')
    #  Inorganics, PAH, PCB, Chlorinated Hydrocarbons, PBDE, Pyrethroid, FIPRONIL, TOC/TN is 1 year (see notes)
    print('#  Inorganics, PAH, PCB, Chlorinated Hydrocarbons, PBDE, Pyrethroid, FIPRONIL, TOC/TN is 1 year (see notes)')

    holding_time_mask = (results.analysisdate - results.sampledate >= timedelta(days=365))
    holding_time_classes = ['Inorganics', 'PAH', 'PCB', 'Chlorinated Hydrocarbons', 'PBDE', 'Pyrethroid', 'FIPRONIL', 'TOC', 'TN']
    results_args.update({
        "badrows": results[
                results.analyteclass.isin(holding_time_classes) 
                & holding_time_mask
            ].tmp_row.tolist(),
        "badcolumn": "SampleDate, AnalysisDate",
        "error_type": "Sample Past Holding Time",
        "error_message": f"Here, the analysisdate is more than a year after the sampledate, which is invalid for analyteclasses {','.join(holding_time_classes)}"
    })
    errs.append(checkData(**results_args))

    # NOTE The Holding time for Mercury is 6 months, so a separate check will be written here specifically for Mercury
    print('# NOTE The Holding time for Mercury is 6 months, so a separate check will be written here specifically for Mercury')
    # months is not allowed in a timedelta so here we put 183 days instead
    print('# months is not allowed in a timedelta so here we put 183 days instead')
    Hg_holding_time_mask = ((results.analysisdate - results.sampledate >= timedelta(days=183)) & (results.analytename == 'Mercury'))
    results_args.update({
        "badrows": results[Hg_holding_time_mask].tmp_row.tolist(),
        "badcolumn": "SampleDate, AnalysisDate",
        "error_type": "Sample Past Holding Time",
        "error_message": f"Here, the analysisdate is more than 6 months after the sampledate, which is past the holding time for Mercury"
    })
    errs.append(checkData(**results_args))
    # ---------------------------------------------------------------------------------------------------------------------------------#
    # ---------------------------------------------------------------------------------------------------------------------------------#
    print('# ---------------------------------------------------------------------------------------------------------------------------------#')
    


    # # ------------------------------------------------------------------------------------------------------------#
    # print('# ------------------------------------------------------------------------------------------------------------#')
    # # Check - For analyteclass Pyrethroid - within the same analysisbatch, you cant have both: - disabled since values do not exist in lookup - zaib 2may2025
    # print('# Check - For analyteclass Pyrethroid - within the same analysisbatch, you cant have both:')
    # # 1. "Deltamethrin/Tralomethrin" and "Deltamethrin"
    # print('# 1. "Deltamethrin/Tralomethrin" and "Deltamethrin"')
    # # 2. "Esfenvalerate/Fenvalerate" and "Esfenvalerate"
    # print('# 2. "Esfenvalerate/Fenvalerate" and "Esfenvalerate"')
    # # 3. "Permethrin, cis" and "Permethrin (cis/trans)"
    # print('# 3. "Permethrin, cis" and "Permethrin (cis/trans)"')
    # # 4. "Permethrin, trans" and "Permethrin (cis/trans)"
    # print('# 4. "Permethrin, trans" and "Permethrin (cis/trans)"')

    # results_args.update(pyrethroid_analyte_logic_check(results, ["Deltamethrin/Tralomethrin", "Deltamethrin"]))
    # errs.append(checkData(**results_args))
    # results_args.update(pyrethroid_analyte_logic_check(results, ["Esfenvalerate/Fenvalerate", "Esfenvalerate"]))
    # errs.append(checkData(**results_args))
    # results_args.update(pyrethroid_analyte_logic_check(results, ["Permethrin, cis", "Permethrin (cis/trans)"]))
    # errs.append(checkData(**results_args))
    # results_args.update(pyrethroid_analyte_logic_check(results, ["Permethrin, trans", "Permethrin (cis/trans)"]))
    # errs.append(checkData(**results_args))
    
    # # END Check - For analyteclass Pyrethroid - within the same analysisbatch, you cant have both .......
    # print('# END Check - For analyteclass Pyrethroid - within the same analysisbatch, you cant have both .......')
    # ------------------------------------------------------------------------------------------------------------#
    print('# ------------------------------------------------------------------------------------------------------------#')

    # Check - If sampletype is a Reference material, the matrix cannot be "labwater" - it must be sediment
    print('# Check - If sampletype is a Reference material, the matrix cannot be "labwater" - it must be sediment')
    results_args.update({
        "badrows": results[results.sampletype.str.contains('Reference', case = False) & (results.matrix != 'sediment')].tmp_row.tolist(),
        "badcolumn": "SampleType, Matrix",
        "error_type": "Value Error",
        "error_message": f"If sampletype is a Reference material, the matrix cannot be 'labwater' - Rather, it must be sediment"
    })
    errs.append(checkData(**results_args))
    
    
    # -----------------------------------------------------------------------------------------------------------------------------------#
    # Check - for PAH's, in the sediment matrix, the units must be ng/g dw (for CRM, ug/g dw or mg/kg dw are acceptable)
    print("# Check - for PAH's, in the sediment matrix, the units must be ng/g dw (for CRM, ug/g dw or mg/kg dw are acceptable)")
    pah_sed_mask = ((results.matrix == 'sediment') & (results.analyteclass == 'PAH'))
    pah_unit_mask = (pah_sed_mask & (~results.sampletype.str.contains('Reference', case = False))) & (results.units != 'ng/g dw')
    pah_unit_crm_mask = (pah_sed_mask & (results.sampletype.str.contains('Reference', case = False))) & (results.units.isin(['ug/g dw', 'mg/kg dw']))
    
    results_args.update({
        "badrows": results[pah_unit_mask].tmp_row.tolist(),
        "badcolumn": "Units",
        "error_type": "Value Error",
        "error_message": f"for PAH's, the units must be ng/g dw"
    })
    errs.append(checkData(**results_args))
    
    results_args.update({
        "badrows": results[pah_unit_crm_mask].tmp_row.tolist(),
        "badcolumn": "Units",
        "error_type": "Value Error",
        "error_message": f"for PAH's, and Reference Material sampletypes, the units must be in ug/g dw or mg/kg dw"
    })
    errs.append(checkData(**results_args))
    # -----------------------------------------------------------------------------------------------------------------------------------#
    

    # -----------------------------------------------------------------------------------------------------------------------------------#
    # Check - for Chlorinated Hydrocarbons, PBDE, PCB in the sediment matrix, the units must be ng/g dw (for CRM, ug/kg dw is also acceptable)
    print('# Check - for Chlorinated Hydrocarbons, PBDE, PCB in the sediment matrix, the units must be ng/g dw (for CRM, ug/kg dw is also acceptable)')
    # (for matrix = sediment)
    sed_mask = ((results.matrix == 'sediment') & (results.analyteclass.isin(['Chlorinated Hydrocarbons', 'PBDE', 'PCB'])))
    unit_mask = (sed_mask & (~results.sampletype.str.contains('Reference', case = False))) & (results.units != 'ng/g dw')
    unit_crm_mask = (sed_mask & (results.sampletype.str.contains('Reference', case = False))) & (results.units.isin(['ng/g dw', 'ug/kg dw']))
    
    results_args.update({
        "badrows": results[unit_mask].tmp_row.tolist(),
        "badcolumn": "Units",
        "error_type": "Value Error",
        "error_message": f"for Chlorinated Hydrocarbons, PBDE, PCB, the units must be ng/g dw"
    })
    errs.append(checkData(**results_args))
    
    results_args.update({
        "badrows": results[unit_crm_mask].tmp_row.tolist(),
        "badcolumn": "Units",
        "error_type": "Value Error",
        "error_message": f"for Chlorinated Hydrocarbons, PBDE, PCB (Reference Material sampletypes), the units must be in ng/g dw or ug/kg dw"
    })
    errs.append(checkData(**results_args))
    # -----------------------------------------------------------------------------------------------------------------------------------#



    # -----------------------------------------------------------------------------------------------------------------------------------#
    # Check - for Pyrethroid, in the sediment matrix, the units must be ng/g dw
    print('# Check - for Pyrethroid, in the sediment matrix, the units must be ng/g dw')
    fip_pyre_mask = ((results.matrix == 'sediment') & (results.analyteclass.isin(['Pyrethroid']))) & (results.units != 'ng/g dw')
    
    results_args.update({
        "badrows": results[fip_pyre_mask].tmp_row.tolist(),
        "badcolumn": "Units",
        "error_type": "Value Error",
        "error_message": f"for Pyrethroid (where matrix = sediment), the units must be ng/g dw"
    })
    # -----------------------------------------------------------------------------------------------------------------------------------

    # ----- END CUSTOM CHECKS - SEDIMENT RESULTS ----- #




    # If there are errors, dont waste time with the QA plan checks
    # For testing, let us not enforce this, or we will waste a lot of time cleaning data
    # if errs != []:
    #     return {'errors': errs, 'warnings': warnings}






    # -=======- BIGHT CHEMISTRY QA PLAN CHECKS -=======- #  
    # Percent Recovery is computed right before the QA Checks since the first stage of custom checks will issue an error for TrueValue as 0. 
    # Calculate percent recovery
    # if truevalue is 0 - critical error: float division by zero BUG
    results['percentrecovery'] = \
        results.apply(
            lambda x: 
            float(x.result)/float(x.truevalue)*100 if ('spike' in x.sampletype.lower())|('reference' in x.sampletype.lower()) else -88, 
            axis = 1
        )

    # ------- Table 5-3 - Inorganics, Non-tissue matrices (Sediment and labwater) -------#

    # --- TABLE 5-3 Check #0 --- #
    # Check - Frequency checks
    print('# Check - Frequency checks')
    # within the batch, there must be 
    
    
    # --- END TABLE 5-3 Check #0 --- #
    
    # --- TABLE 5-3 Check #1 --- #
    # Check - 15 Analytes must be in each grouping of AnalysisBatchID, SampleID, sampletype, and labreplicate
    # NOTE: Remains the same in bight 2023
    print('# Check - 15 Analytes must be in each grouping of AnalysisBatchID, SampleID, sampletype, and labreplicate')
    #   (if that batch is analyzing inorganics) (ERROR)

    # The filter mask to be used throughout the whole table 5-3 checks
    inorg_sed_mask = (results.analyteclass == 'Inorganics') & results.matrix.isin(['sediment','labwater', 'Ottawa sand'])


    # --- END TABLE 5-3 Check #1 --- #
    # Covered above
    if not results[inorg_sed_mask].empty:
        # --- TABLE 5-3 Check #2 --- #
        # Check - For the SampleType "Reference - ERA 540 Sed" - Result should match lu_chemcrm range (PT acceptance limits)
        # NOTE: I need the updated range values to update the lookup table - March 14, 2023 - Robert
        print('# Check - For the SampleType "Reference - ERA 540 Sed" - Result should match lu_chemcrm range (PT acceptance limits)')
        # In my understanding, its mainly for the reference material for inorganics in the sediment matrix, rather than a particular CRM
        # UPDATE - the only CRM for metals in sediment, is ERA 540
        inorg_sed_ref_mask = inorg_sed_mask & (results.sampletype == "Reference - ERA 540 Sed")
        
        crmvals = pd.read_sql(
            f"""
            SELECT 
                analytename, 
                pt_performance_lowerbound AS lower_bound,
                pt_performance_upperbound AS upper_bound
            FROM lu_chemcrm 
            WHERE crm = 'Reference - ERA 540 Sed'
            """,
            eng
        )
        
        checkdf = results[inorg_sed_ref_mask].merge(crmvals, on = 'analytename', how = 'inner')

        badrows = checkdf[
            checkdf.apply(
                lambda row: (row.result < row.lower_bound ) | (row.result > row.upper_bound),
                axis = 1
            )
        ].tmp_row.tolist()
        
        results_args.update({
            "badrows": badrows,
            "badcolumn": "Result",
            "error_type": "Value Error",
            "error_message": f"The value here is outside the PT performance limits for ERA 540 (<a href=https://nexus.sccwrp.org/bight23checker/scraper?action=help&layer=lu_chemcrm target=_blank>See the CRM Lookup lsit values</a>)"
        })
        warnings.append(checkData(**results_args))
        # --- END TABLE 5-3 Check #2 --- #


        # --- TABLE 5-3 Check #3 --- #
        # Check - For Method blank sampletypes - Result < MDL or Result < 5% of measured concentration in samples (Warning)
        # NOTE: Remains the same in Bight 2023
        print('# Check - For Method blank sampletypes - Result < MDL or Result < 5% of measured concentration in samples (Warning)')
        argslist = MB_ResultLessThanMDL(results[inorg_sed_mask])
        print("done calling MB ResultLessThanMDL")
        for args in argslist:
            results_args.update(args)
            warnings.append(checkData(**results_args))
        # --- END TABLE 5-3 Check #3 --- #


        # --- TABLE 5-3 Check #4 --- #
        # Sample Duplicate or Matrix spike duplicate required for 10% of the samples in a batch
        tmp = results[inorg_sed_mask].groupby(['analysisbatchid', 'sampleid','analytename']).apply(
            lambda df: 
            not df[(df.labreplicate == 2) & df.sampletype.isin(['Matrix spike','Result'])].empty
        ) \
        .reset_index(name = 'has_dup')

        # identify samples where not all analytes had their duplicates
        tmp = tmp.groupby(['analysisbatchid','sampleid']).agg({'has_dup': all}).reset_index()
        
        # get percentage of samples within batch that had all analytes with their dupes
        tmp = tmp.groupby('analysisbatchid') \
            .agg({'has_dup': lambda x: sum(x) / len(x)}) \
            .reset_index() \
            .rename(columns = {'has_dup':'percent_samples_with_dupes'})
        
        # batches where 
        badbatches = tmp[tmp.percent_samples_with_dupes < 0.1]

        bad = results[results.analysisbatchid.isin(badbatches.analysisbatchid.tolist())]
        if not bad.empty:
            bad = bad.groupby('analysisbatchid').agg({'tmp_row': list}).reset_index()
            for _, row in bad.iterrows():
                results_args.update({
                    "badrows": row.tmp_row, # list of rows associated with the batch that doesnt meet the matrix/sample dup requirement
                    "badcolumn": "SampleType",
                    "error_type": "Incomplete data",
                    "error_message": f"Under 10% of samples in the batch {row.analysisbatchid} have a sample duplicate / matrix spike duplicate"
                })
                warnings.append(checkData(**results_args))

        # --- END TABLE 5-3 Check #4 --- #


        # --- TABLE 5-3 Check #5 --- #
        # Check - At least one blank spike result per batch should be within 15% of the TrueValue (85 to 115 percent recovery)

        print('# Check - At least one Blank spike result per batch should be within 15% of the TrueValue (85 to 115 percent recovery)')
        # It is checking to see if all analytes in either the blank spike, or the duplicate, were inside of 15% of the TrueValue
        # I need to confirm that this is what it is supposed to do
        
        pct_recovery_thresh = 15
        checkdf = results[inorg_sed_mask & results.sampletype.str.contains('Blank spiked', case = False)] \
            .groupby(['analysisbatchid', 'sampleid','labreplicate']) \
            .apply(
                lambda df: 
                all((df.percentrecovery.between(100 - pct_recovery_thresh, 100 + pct_recovery_thresh)))
            )
        if not checkdf.empty:
            checkdf = checkdf.reset_index(name = 'passed_within15_check')

            # only bad analysis batches will remain
            checkdf = results[inorg_sed_mask & results.sampletype.str.contains('Blank spiked', case = False) ] \
                .merge(checkdf[~checkdf.passed_within15_check], on = 'analysisbatchid', how = 'inner')

            results_args.update({
                "badrows": checkdf.tmp_row.tolist(),
                "badcolumn": "Result",
                "error_type": "Value Error",
                "error_message": "Within this analysisbatch, at least one of the Blank spike sets should have had all their percent recoveries within 15 percent"
            })
            warnings.append(checkData(**results_args))
        # --- End Table 5-3 check #5 --- #
        

        # --- TABLE 5-3 Check "#6" (a, b, and c) --- #
        # Check - Duplicate Matrix spikes (or Results) need < 20% RPD for AnalysisMethods ICPAES, EPA200.7 and EPA 6010B
        print('# Check - Duplicate Matrix spikes (or Results) need < 20% RPD for AnalysisMethods ICPAES, EPA200.7 and EPA 6010B')
        
        # QUESTION - Mercury seems to often be analyzed with the method EPA245.7m - what is the RPD threshold on that?
        # Based on the bight 2018 checker, it looks like Bowen told us it had to be under 30 - thats what the old one does

        # NOTE (March 14, 2023): This will change
        # for 'ICPAES', 'EPA200.7', 'EPA 6010B' it says "10% (within 3 standard deviations)" but lets set it to 25% rpd
        # for 'ICPMS', 'EPA200.8', 'EPA 6020Bm' it says within 25% RPD
        # for 'CVAA','FAA','GFAA','HAA','EPA245.7m','EPA245.5','EPA7473','SW846 7471','EPA7471B' it says within 30% RPD
        
        icpaes_methods = ['ICPAES', 'EPA200.7', 'EPA6010D','EPA6010B'] # methods that require 20% RPD - Inductively Coupled Plasma Atomic Emission Spectrometry
        icpaes_tolerance = .25 
        icpaes_blankspike_tolerance = 0.25
        icpms_methods = ['ICPMS', 'EPA200.8', 'EPA 6020Bm'] # methods that require 30% RPD - Inductively Coupled Plasma Mass Spectrometry
        icpms_tolerance = .25 
        icpms_blankspike_tolerance = 0.15
        aa_methods = ['CVAA','FAA','GFAA','HAA','EPA245.7m','EPA245.5','EPA7473','SW846 7471','EPA7471B'] # - Atomic Absorbtion
        aa_tolerance = .3 

        rpdcheckmask = (
            inorg_sed_mask 
            & (
                results.sampletype.isin(['Matrix spike', 'Result', 'Blank spiked']) 
            )
        )
        checkdf = results[rpdcheckmask]
        checkdf = checkdf.assign(
            tolerance = checkdf.apply( 
                lambda row: 
                icpaes_tolerance
                if ( (row.analysismethod in icpaes_methods) and (row.sampletype in ['Result','Matrix spike'] ) ) 
                else icpaes_blankspike_tolerance
                if ( (row.analysismethod in icpaes_methods) and (row.sampletype in ['Blank spiked'] ) ) 
                else icpms_tolerance
                if ( (row.analysismethod in icpms_methods) and (row.sampletype in ['Result','Matrix spike'] ) ) 
                else icpms_blankspike_tolerance
                if ( (row.analysismethod in icpms_methods) and (row.sampletype in ['Blank spiked'] ) ) 
                else aa_tolerance
                if ( (row.analysismethod in aa_methods) and (row.sampletype in ['Result','Matrix spike'] ) ) 
                else pd.NA
                ,
                axis = 1
            ),
            analysismethodgroup = checkdf.analysismethod.apply( 
                lambda x: 'ICPAES' if x in icpaes_methods else 'ICPMS' if x in icpms_methods else 'AA'
            )
        )

        # drop records where the tolerance ended up as pd.NA
        checkdf.dropna(subset = 'tolerance', inplace = True)

        # stationid and sampledate essentially functions as the sampleid
        checkdf = checkdf.groupby(['analysisbatchid', 'analysismethod', 'analysismethodgroup', 'sampletype', 'analytename','sampleid', 'tolerance']).apply(
            lambda subdf:
            abs((subdf.result.max() - subdf.result.min()) / ((subdf.result.max() + subdf.result.min()) / 2))
        )
        if not checkdf.empty:
            checkdf = checkdf.reset_index(name = 'rpd')
            checkdf['errmsg'] = checkdf.apply(
                lambda row:
                (
                    f"For the AnalysisMethod {row.analysismethod}, "
                    f"{'Matrix spike' if row.sampletype == 'Matrix spike' else 'Blank spike' if row.sampletype == 'Blank spiked' else 'Sample'}"
                    f" duplicates should have an RPD under {(row.tolerance) * 100}%"
                )
                , axis = 1
            )
            checkdf = results[rpdcheckmask] \
                .merge(
                    checkdf[
                        # just merge records that failed the check
                        # We never multiplied RPD by 100, so it should be expressed as a decimal here
                        checkdf.apply(lambda x: x.rpd > x.tolerance, axis = 1)
                    ], 
                    on = ['analysisbatchid','analysismethod','sampletype','analytename','stationid', 'sampledate'], 
                    how = 'inner'
                )
            
            tmp = checkdf.groupby(['errmsg']) \
                .apply(lambda df: df.tmp_row.tolist())
            if not tmp.empty:
                argslist = tmp \
                    .reset_index(name = 'badrows') \
                    .apply(
                        lambda row: 
                        {
                            "badrows": row.badrows,
                            "badcolumn": "Result",
                            "error_type": "Value Error",
                            "error_message": row.errmsg
                        },
                        axis = 1
                    ).tolist()

                for args in argslist:
                    results_args.update(args)
                    warnings.append(checkData(**results_args))


            
        # --- END TABLE 5-3 Check --- # (# Check - Duplicate Matrix spikes (or Results) need < 20% RPD for AnalysisMethods ICPAES, EPA200.7 and EPA 6010B)
        print("# --- END TABLE 5-3 Check --- # (# Check - Duplicate Matrix spikes (or Results) need < 20% RPD for AnalysisMethods ICPAES, EPA200.7 and EPA 6010B)")
        
        
        # --- TABLE 5-3 Check --- #
        # --- Table 5-3 - AnalysisMethods ICPAES and ICPMS, blank spike duplicates are required --- #
        print("# --- Table 5-3 - AnalysisMethods ICPAES and ICPMS, blank spike duplicates are required --- #")
        tmp_orig = results[inorg_sed_mask & results.analysismethod.isin([*icpaes_methods, *icpms_methods])] 
        tmp = tmp_orig.groupby(['analysisbatchid', 'analytename']).apply(
            lambda df:
            not df[(df.sampletype == 'Blank spiked') & (df.labreplicate == 2)].empty # signifies whether or not a blank spiked duplicate is present
        )
        if not tmp.empty:
            tmp = tmp.reset_index( name = 'has_blankspike_dup') 
            tmp = tmp[~tmp.has_blankspike_dup] # get batches without the blank spike dupes
            tmp = tmp_orig.merge(tmp, on = ['analysisbatchid', 'analytename'], how = 'inner')
            tmp = tmp.groupby(['analysisbatchid', 'analytename']).agg({'tmp_row': list})
            if not tmp.empty:
                tmp = tmp.reset_index()
                for _, row in tmp.iterrows():
                    results_args.update({
                        "badrows": row.tmp_row, # list of rows associated with the batch that doesnt have a blank spike dup
                        "badcolumn": "SampleType",
                        "error_type": "Incomplete data",
                        "error_message": f"The batch {row.analysisbatchid} is missing a blank spike duplicate for {row.analytename} (since it is a batch for metals with analysismethod ICPAES or ICPMS)"
                    })
                    warnings.append(checkData(**results_args))




        # ------- END Table 5-3 - Inorganics, Non-tissue matrices (Sediment and labwater) -------#
        print("# ------- END Table 5-3 - Inorganics, Non-tissue matrices (Sediment and labwater) -------#")




    # ------- Table 5-4 - PAH, Non-tissue matrices (Sediment and labwater) -------#
    print("# ------- Table 5-4 - PAH, Non-tissue matrices (Sediment and labwater) -------#")
    # The filter mask to be used throughout the whole table 5-4 checks
    pah_sed_mask = (results.analyteclass == 'PAH') & results.matrix.isin(['sediment','labwater', 'Ottawa sand'])


    if not results[pah_sed_mask].empty:
        # --- TABLE 5-4 Check #1 --- #
        # Check - Make sure they have all the required PAH anlaytes
        print('# Check - Make sure they have all the required PAH anlaytes')
    
        # 24 required analytes from the PAH analyteclass
        req_analytes_tbl54 = pd.read_sql("SELECT * FROM lu_analytes WHERE analyteclass = 'PAH'", eng).analyte.tolist()

        # --- END TABLE 5-4 Check #1 --- #
        # Covered above

        # --- TABLE 5-4 Check #2 --- #
        # Check - For reference materials - Result should be within 40% of the specified value (in lu_chemcrm) for 80% of the analytes
        # print('# Check - For reference materials - Result should be within 40% of the specified value (in lu_chemcrm) for 80% of the analytes')
        crmvals = pd.read_sql(
            f"""
            SELECT analyte AS analytename, reference_value FROM lu_chemcrm 
            WHERE analyte IN ('{"','".join(req_analytes_tbl54).replace(';','')}')
            AND matrix = 'sediment'
            """,
            eng
        )
        checkdf = results[pah_sed_mask & results.sampletype.str.contains('Reference', case = False)] 
        if not checkdf.empty:
            checkdf = checkdf.merge(crmvals, on = 'analytename', how = 'inner')
        
        if not checkdf.empty:
            checkdf['within40pct'] = checkdf.apply(
                    lambda row:
                    (0.6 * float(row.reference_value)) <= row.result <= (1.4 * float(row.reference_value)) if not pd.isnull(row.reference_value) else True,
                    axis = 1
                )
            checkdf = checkdf.merge(
                checkdf.groupby('analysisbatchid') \
                    .apply(
                        lambda df: sum(df.within40pct) / len(df) < 0.8
                    ) \
                    .reset_index(name = 'failedcheck'),
                on = 'analysisbatchid',
                how = 'inner'
            )
            checkdf = checkdf[checkdf.failedcheck]
            results_args.update({
                "badrows": checkdf.tmp_row.tolist(),
                "badcolumn": "AnalysisBatchID",
                "error_type": "Value Error",
                "error_message": "Less than 80% of the analytes in this batch are within 40% of the CRM value"
            })
            warnings.append(checkData(**results_args))

        # --- END TABLE 5-4 Check #2 --- #
        print("# --- END TABLE 5-4 Check #2 --- #")
        
        

        # --- TABLE 5-4 Check #3 --- #
        # Check - Matrix spike duplicate required (1 per batch)
        print('# Check - Matrix spike duplicate required (1 per batch)')
        tmp_orig = results[pah_sed_mask] 
        tmp = tmp_orig.groupby(['analysisbatchid', 'analytename']).apply(
            lambda df:
            not df[(df.sampletype == 'Matrix spike') & (df.labreplicate == 2)].empty # signifies whether or not a Matrix spike duplicate is present
        )
        if not tmp.empty:
            tmp = tmp.reset_index( name = 'has_matrixspike_dup') 
            tmp = tmp[~tmp.has_matrixspike_dup] # get batches without the matrix spike dupes
            tmp = tmp_orig.merge(tmp, on = ['analysisbatchid', 'analytename'], how = 'inner')
            tmp = tmp.groupby(['analysisbatchid', 'analytename']).agg({'tmp_row': list})
            if not tmp.empty:
                tmp = tmp.reset_index()
                for _, row in tmp.iterrows():
                    results_args.update({
                        "badrows": row.tmp_row, # list of rows associated with the batch that doesnt have a matrix spike dup
                        "badcolumn": "SampleType",
                        "error_type": "Incomplete data",
                        "error_message": f"The batch {row.analysisbatchid} is missing a matrix spike duplicate for {row.analytename}"
                    })
                    warnings.append(checkData(**results_args))
        # --- END TABLE 5-4 Check #3 --- #
        print("# --- END TABLE 5-4 Check #3 --- #")


        


        # --- TABLE 5-4 Check #4 --- #
        # Check - Duplicate Matrix spikes must have RPD < 40% for 70% of the analytes
        print('# Check - Duplicate Matrix spikes must have RPD < 40% for 70% of the analytes')
        checkdf = results[pah_sed_mask & results.sampletype.str.contains('Matrix spike', case = False)]
        checkdf = checkdf.groupby(['analysisbatchid', 'analytename','sampleid']).apply(
            lambda subdf:
            abs((subdf.result.max() - subdf.result.min()) / ((subdf.result.max() + subdf.result.min()) / 2)) <= 0.4
        )

        if not checkdf.empty:
            checkdf = checkdf.reset_index(name = 'rpd_under_40')
            checkdf = checkdf.groupby('analysisbatchid').apply(lambda df: sum(df.rpd_under_40) / len(df) >= 0.7 )
            if not checkdf.empty:
                checkdf = checkdf.reset_index(name = 'passed')
                checkdf['errmsg'] = checkdf.apply(
                    lambda row:
                    f"Duplicate Matrix spikes should have an RPD under 40% for 70% of the analytes in the batch"
                    , axis = 1
                )
                checkdf = results[pah_sed_mask & results.sampletype.str.contains('Matrix spike', case = False)] \
                    .merge(checkdf[~checkdf.passed], on = ['analysisbatchid'], how = 'inner')
                
                argslist = checkdf.groupby(['errmsg']) \
                    .apply(lambda df: df.tmp_row.tolist())
                
                if not argslist.empty:
                    argslist = argslist \
                        .reset_index(name = 'badrows') \
                        .apply(
                            lambda row: 
                            {
                                "badrows": row.badrows,
                                "badcolumn": "Result",
                                "error_type": "Value Error",
                                "error_message": row.errmsg
                            },
                            axis = 1
                        ).tolist()

                    for args in argslist:
                        results_args.update(args)
                        warnings.append(checkData(**results_args))

        # --- END TABLE 5-4 Check #4 --- #
        print("# --- END TABLE 5-4 Check #4 --- #")
        
        
        # --- TABLE 5-4 Check #5 and 6 --- #
        # Check - within an analysisbatch, Matrix spikes should have 60-140% recovery of spiked mass for 80% of analytes
        print('# Check - within an analysisbatch, Matrix spikes/Blank spikes should have 60-140% recovery of spiked mass for 80% of analytes')
        checkdf = results[pah_sed_mask & results.sampletype.isin(['Matrix spike', 'Blank spiked'])] \
            .groupby(['analysisbatchid', 'sampletype', 'sampleid', 'labreplicate']) \
            .apply(
                lambda df: 
                (sum((df.percentrecovery > 60) & (df.percentrecovery < 140)) / len(df)) >= 0.8
            )
        if not checkdf.empty:
            checkdf = checkdf.reset_index(name = 'passed_check')
            checkdf = results.merge(checkdf, on = ['analysisbatchid', 'sampletype', 'sampleid', 'labreplicate'], how = 'inner')
            checkdf = checkdf[checkdf.sampletype.isin(['Matrix spike', 'Blank spiked'])]
            checkdf = checkdf[(~checkdf.passed_check) & ((checkdf.percentrecovery < 60) | (checkdf.percentrecovery > 140))]

            # changed sampleid to labsamplid inside badcolumns --- TEST
            results_args.update({
                "badrows": checkdf.tmp_row.tolist(),
                "badcolumn": "AnalysisBatchID, SampleType, LabSampleID, LabReplicate, Result",
                "error_type": "Value Error",
                "error_message": f"For Matrix spikes/Blank spikes, more than 80% of analytes should have 60-140% recovery"
            })
            warnings.append(checkData(**results_args))
        # --- END TABLE 5-4 Check #5 --- #
        print("# --- END TABLE 5-4 Check #5 --- #")



        # --- TABLE 5-4 Check #7 --- #
        # Check - For SampleType = Lab blank, we must require Result < 10 * MDL - if that criteria is met, the qualifier should be "none"
        print('# Check - For SampleType = Lab blank, we must require Result < 10 * MDL - if that criteria is met, the qualifier should be "none"')
        
        # First check that the result is under 10 times the MDL
        badrows = results[(pah_sed_mask & (results.sampletype == 'Lab blank')) & (results.result >= (10 * results.mdl))].tmp_row.tolist()
        results_args.update({
            "badrows": badrows,
            "badcolumn": "Result",
            "error_type": "Value Error",
            "error_message": f"For Lab blanks, the result must be less than 10 times the MDL (for PAH)"
        })
        warnings.append(checkData(**results_args))

        # If the requirement is met, check that the qualifier says none
        badrows = results[
            ((pah_sed_mask & results.sampletype == 'Lab blank') & (results.result < (10 * results.mdl))) & 
            (results.qualifier != 'none')
        ].tmp_row.tolist()

        results_args.update({
            "badrows": badrows,
            "badcolumn": "Qualifier",
            "error_type": "Value Error",
            "error_message": f"For Lab blanks, if the result is less than 10 times the MDL, then the qualifier should say 'none' (for PAH)"
        })
        warnings.append(checkData(**results_args))
        # --- END TABLE 5-4 Check #7 --- #


        # --- TABLE 5-4 Check # --- #
        # Check - 
        print('# Check - ')
        # --- END TABLE 5-4 Check # --- #

    # ------- END Table 5-4 - PAH, Non-tissue matrices (Sediment and labwater) -------#
    print("# ------- END Table 5-4 - PAH, Non-tissue matrices (Sediment and labwater) -------#")




    # ------- Table 5-5 - Pyrethroids, PCB, PBDE, Chlorinated Hydrocarbons, Non-tissue matrices (Sediment and labwater) -------#
    print("# ------- Table 5-5 - Pyrethroids, PCB, PBDE, Chlorinated Hydrocarbons, Non-tissue matrices (Sediment and labwater) -------#")

    analyteclasses55 = ['PCB','PBDE','Chlorinated Hydrocarbons','Pyrethroid','Neonicotinoids','PFAS','TIREWEAR']
    mask55 = results.analyteclass.isin(analyteclasses55)
    results55 = results[mask55]
    
    print("results55")
    print(results55)
    if not results55.empty:
        # --- TABLE 5-5 Check #1 --- #
        # Check - check for all required sampletypes
        # covered above
        # --- END TABLE 5-5 Check #1 --- #
        

        # --- TABLE 5-5 Check #2 --- #
        # Check - For reference materials - Result should be within 40% of the specified value (in lu_chemcrm) for 70% of the analytes
        print('# Check - For reference materials - Result should be within 40% of the specified value (in lu_chemcrm) for 70% of the analytes')
        crmvals = pd.read_sql(
            f"""
            SELECT
                lu_chemcrm.analytename,
                lu_chemcrm.matrix,
                lu_chemcrm.certified_value,
                lu_analytes.analyteclass 
            FROM
                lu_chemcrm
                JOIN lu_analytes ON lu_chemcrm.analytename = lu_analytes.analyte 
            WHERE
                lu_analytes.analyteclass IN ( '{"','".join(analyteclasses55)}' ) 
                AND matrix = 'sediment'
            """,
            eng
        )
        checkdf = results[mask55 & results.sampletype.str.contains('Reference', case = False)]
        if not checkdf.empty:
            checkdf = checkdf.merge(crmvals, on = 'analytename', how = 'left')
        
        if not checkdf.empty:
            checkdf['within40pct'] = checkdf.apply(
                    lambda row:
                    (0.6 * float(row.certified_value)) <= row.result <= (1.4 * float(row.certified_value)) if not pd.isnull(row.certified_value) else True
                    ,axis = 1
                )
            checkdf = checkdf.merge(
                checkdf.groupby('analysisbatchid') \
                    .apply(
                        lambda df: sum(df.within40pct) / len(df) < 0.7
                    ) \
                    .reset_index(name = 'failedcheck'),
                on = 'analysisbatchid',
                how = 'inner'
            )
            checkdf = checkdf[checkdf.failedcheck]
            results_args.update({
                "badrows": checkdf.tmp_row.tolist(),
                "badcolumn": "AnalysisBatchID",
                "error_type": "Value Error",
                "error_message": "Less than 70% of the analytes in this batch are within 40% of the CRM value"
            })
            warnings.append(checkData(**results_args))
        # --- END TABLE 5-5 Check #2 --- #
        print("# --- END TABLE 5-5 Check #2 --- #")

        # --- TABLE 5-5 Check #3, #6 --- #
        # Check - Matrix spike duplicate required (1 per batch)
        print('# Check - Matrix spike duplicate required (1 per batch)')
        tmp = results55.groupby(['analysisbatchid', 'analytename']).apply(
            lambda df:
            not df[(df.sampletype == 'Matrix spike') & (df.labreplicate == 2)].empty # signifies whether or not a Matrix spike duplicate is present
        )
        if not tmp.empty:
            tmp = tmp.reset_index( name = 'has_matrixspike_dup') 
            tmp = tmp[~tmp.has_matrixspike_dup] # get batches without the matrix spike dupes
            tmp = results55.merge(tmp, on = ['analysisbatchid', 'analytename'], how = 'inner')
            tmp = tmp.groupby(['analysisbatchid', 'analytename']).agg({'tmp_row': list})
            if not tmp.empty:
                tmp = tmp.reset_index()
                for _, row in tmp.iterrows():
                    results_args.update({
                        "badrows": row.tmp_row, # list of rows associated with the batch that doesnt have a matrix spike dup
                        "badcolumn": "SampleType",
                        "error_type": "Incomplete data",
                        "error_message": f"The batch {row.analysisbatchid} is missing a matrix spike duplicate for {row.analytename}"
                    })
                    warnings.append(checkData(**results_args))
        
        #(Check #6, sample as check #3 except with Blank spikes)
        print('# Check - Blank spike duplicate required (1 per batch)')
        tmp = results55.groupby(['analysisbatchid', 'analytename']).apply(
            lambda df:
            not df[(df.sampletype == 'Blank spiked') & (df.labreplicate == 2)].empty # signifies whether or not a Matrix spike duplicate is present
        )
        if not tmp.empty:
            tmp = tmp.reset_index( name = 'has_blankspike_dup') 
            tmp = tmp[~tmp.has_blankspike_dup] # get batches without the matrix spike dupes
            tmp = results55.merge(tmp, on = ['analysisbatchid', 'analytename'], how = 'inner')
            tmp = tmp.groupby(['analysisbatchid', 'analytename']).agg({'tmp_row': list})
            if not tmp.empty:
                tmp = tmp.reset_index()
                for _, row in tmp.iterrows():
                    results_args.update({
                        "badrows": row.tmp_row, # list of rows associated with the batch that doesnt have a matrix spike dup
                        "badcolumn": "SampleType",
                        "error_type": "Incomplete data",
                        "error_message": f"The batch {row.analysisbatchid} is missing a blank spike duplicate for {row.analytename}"
                    })
                    warnings.append(checkData(**results_args))
        # --- END TABLE 5-5 Check #3 --- #
        print("# --- END TABLE 5-5 Check #3 --- #")


        # --- TABLE 5-5 Check #4, #7 --- #
        # Check - Within an analysisbatch, Matrix spikes/Blank spikes should have 60-140% recovery of spiked mass for 70% of analytes (WARNING)
        print('# Check - Within an analysisbatch, Matrix spikes/Blank spikes should have 60-140% recovery of spiked mass for 70% of analytes (WARNING)')
        checkdf = results[mask55 & results.sampletype.isin(['Matrix spike', 'Blank spiked'])] \
            .groupby(['analysisbatchid', 'sampletype', 'analyteclass','sampleid','labreplicate']) \
            .apply(
                lambda df: 
                (sum((df.percentrecovery > 60) & (df.percentrecovery < 140)) / len(df)) >= 0.7
            )
        if not checkdf.empty:
            checkdf = checkdf.reset_index(name = 'passed_check')
            checkdf = results.merge(checkdf, on = ['analysisbatchid', 'sampletype', 'analyteclass','sampleid','labreplicate'], how = 'inner')
            checkdf = checkdf[checkdf.sampletype.isin(['Matrix spike', 'Blank spiked'])]
            checkdf = checkdf[(~checkdf.passed_check) & ((checkdf.percentrecovery < 60) | (checkdf.percentrecovery > 140))]

            results_args.update({
                "badrows": checkdf.tmp_row.tolist(),
                "badcolumn": "AnalysisBatchID, SampleType, LabSampleID, LabReplicate, Result",
                "error_type": "Value Error",
                "error_message": f"For Matrix/blank spikes, over 70% of analytes should have 60-140% recovery"
            })
            warnings.append(checkData(**results_args))
        # --- END TABLE 5-5 Check #4 --- #
        print("# --- END TABLE 5-5 Check #4 --- #")
        
        # --- TABLE 5-5 Check #5, #8 --- #
        # Check - Duplicate Matrix spikes must have RPD < 40% for 70% of the analytes
        print('# Check - Duplicate Matrix spikes must have RPD < 40% for 70% of the analytes')
        checkdf = results[mask55 & results.sampletype.isin(['Matrix spike', 'Blank spiked'])]
        checkdf = checkdf.groupby(['analysisbatchid', 'analyteclass', 'sampletype', 'analytename','sampleid']).apply(
            lambda subdf:
            abs((subdf.result.max() - subdf.result.min()) / ((subdf.result.max() + subdf.result.min()) / 2)) <= 0.4
        )

        if not checkdf.empty:
            checkdf = checkdf.reset_index(name = 'rpd_under_40')
            checkdf = checkdf.groupby(['analysisbatchid','analyteclass']).apply(lambda df: sum(df.rpd_under_40) / len(df) >= 0.7 )
            if not checkdf.empty:
                checkdf = checkdf.reset_index(name = 'passed')
                checkdf['errmsg'] = checkdf.apply(
                    lambda row:
                    f"Duplicate Matrix spikes/Blank spikes should have an RPD under 40% for 70% of the analytes in the batch ({row.analysisbatchid}) (for the analyteclass {row.analyteclass})"
                    , axis = 1
                )
                checkdf = results[mask55 & results.sampletype.isin(['Matrix spike', 'Blank spiked'])] \
                    .merge(checkdf[~checkdf.passed], on = ['analysisbatchid', 'analyteclass'], how = 'inner')
                
                if not checkdf.empty:
                    argslist = checkdf.groupby(['errmsg']) \
                        .apply(lambda df: df.tmp_row.tolist()) \
                        .reset_index(name = 'badrows') \
                        .apply(
                            lambda row: 
                            {
                                "badrows": row.badrows,
                                "badcolumn": "Result",
                                "error_type": "Value Error",
                                "error_message": row.errmsg
                            },
                            axis = 1
                        ).tolist()

                    for args in argslist:
                        results_args.update(args)
                        warnings.append(checkData(**results_args))
        # --- END TABLE 5-5 Check #5 --- #
        print("# --- END TABLE 5-5 Check #5 --- #")



        # --- TABLE 5-5 Check #9 --- #
        # Check - For Method Blanks, result has to be less than 10 * MDL and the Result must be less than the RL (WARNING)
        print('# Check - For Method Blanks, result has to be less than 10 * MDL and the Result must be less than the RL (WARNING)')
        #   if that criteria is met then the qualifier should be 'none'

        tmpdf = results55[results55.sampletype == 'Method blank']
        badrows = tmpdf[(tmpdf.result >= (10 * tmpdf.mdl)) | (tmpdf.result >= tmpdf.rl)].tmp_row.tolist()
        results_args.update({
            "badrows": badrows,
            "badcolumn": "Result",
            "error_type": "Value Error",
            "error_message": "For Method blanks, the result should be less than 10 times the MDL, and less than the RL"
        })
        warnings.append(checkData(**results_args))
        
        # Second part of the check - if the criteria is met then the qualifier should be "none"
        badrows = tmpdf[((tmpdf.result < 10 * tmpdf.mdl) & (tmpdf.result < tmpdf.rl)) & (tmpdf.qualifier.str.lower() != 'none')].tmp_row.tolist()
        results_args.update({
            "badrows": badrows,
            "badcolumn": "Qualifier",
            "error_type": "Value Error",
            "error_message": "For Method blanks if the result is less than 10 times the MDL, and less than the RL, then the qualifer should say 'none'"
        })
        warnings.append(checkData(**results_args))

        # --- END TABLE 5-5 Check #9 --- #
        print("# --- END TABLE 5-5 Check #9 --- #")

        # --- TABLE 5-5 Check # --- #
        # Check - 
        print('# Check - ')
        # --- END TABLE 5-5 Check # --- #
    
    
    # ------- END Table 5-5 - Pyrethroids, PCB, PBDE, Chlorinated Hydrocarbons, Non-tissue matrices (Sediment and labwater) -------#
    
    
    # ------- END Table 5-6 - TOC and TN, Non-tissue matrices (Sediment and labwater) -------#
    
    # --- TABLE 5-6 Check #1 --- #
    # Check for all required sampletypes (covered above)
    # --- END TABLE 5-6 Check #1 --- #


    # --- TABLE 5-6 Check #2 --- #
    # Check if the value is within 20% of the CRM values (for the reference materials)
    print("# Check if the value is within 20% of the CRM values (for the reference materials)")
    
    # crmvals dataframe has been defined above, in section 5-3

    checkdf = results[(results.analytename == 'TOC') & (results.sampletype == 'Reference - SRM 1944 Sed')]
    if not checkdf.empty:
        crmvals = pd.read_sql(
            f"""
            SELECT * FROM lu_chemcrm 
            WHERE 
                crm = 'Reference - SRM 1944 Sed'
                AND analytename = 'TOC'
            """,
            eng
        )
        checkdf = checkdf.merge(crmvals, on = 'analytename', how = 'left') 
        checkdf = checkdf.assign(failedcheck = ((checkdf.certified_value * 0.8 > checkdf.result) | (checkdf.result > checkdf.certified_value * 1.2)))

        checkdf = checkdf[checkdf.failedcheck]
        results_args.update({
            "badrows": checkdf.tmp_row.tolist(),
            "badcolumn": "Result",
            "error_type": "Value Error",
            "error_message": f"The result should be within 20% of the certified value in <a href=/{current_app.config.get('APP_SCRIPT_ROOT')}/scraper?action=help&layer=lu_chemcrm>lu_chemcrm</a>"
        })
        warnings.append(checkData(**results_args))

    # --- END TABLE 5-6 Check #2 --- #
    print("# --- END TABLE 5-6 Check #2 --- #")



    # --- TABLE 5-6 Check #3 --- #
    # Check - For SampleType = Method blank, we must require Result < 10 * MDL (WARNING)
    print('# Check - For SampleType = Method blank, we must require Result < 10 * MDL (WARNING)')
    #   if that criteria is met, the qualifier should be "none" (WARNING)
    # First check that the result is under 10 times the MDL
    badrows = results[
        ((results.analyteclass.isin(['TOC','TN'])) & (results.sampletype == 'Method blank')) & (results.result >= (10 * results.mdl))
    ].tmp_row.tolist()
    results_args.update({
        "badrows": badrows,
        "badcolumn": "Result",
        "error_type": "Value Error",
        "error_message": f"For Method blanks, the result must be less than 10 times the MDL (for TOC and TN)"
    })
    warnings.append(checkData(**results_args))

    # If the requirement is met, check that the qualifier says none
    badrows = results[
        (((results.analyteclass.isin(['TOC','TN'])) & (results.sampletype == 'Method blank')) & (results.result < (10 * results.mdl)))
        & 
        (results.qualifier != 'none')
    ].tmp_row.tolist()

    results_args.update({
        "badrows": badrows,
        "badcolumn": "Qualifier",
        "error_type": "Value Error",
        "error_message": f"For Method blanks, if the result is less than 10 times the MDL, then the qualifier should say 'none' (for TOC and TN)"
    })
    warnings.append(checkData(**results_args))
    
    # --- END TABLE 5-6 Check #3 --- #

    # --- Table 5-6 Check #4 --- #
    print('# Check - Sample duplicate required (1 per batch)')
    tmp = results55.groupby(['analysisbatchid', 'analytename']).apply(
        lambda df:
        not df[(df.sampletype == 'Result') & (df.labreplicate == 2)].empty # signifies whether or not a Matrix spike duplicate is present
    )
    if not tmp.empty:
        tmp = tmp.reset_index( name = 'has_sample_dup') 
        tmp = tmp[~tmp.has_sample_dup] # get batches without the matrix spike dupes
        tmp = results55.merge(tmp, on = ['analysisbatchid', 'analytename'], how = 'inner')
        tmp = tmp.groupby(['analysisbatchid', 'analytename']).agg({'tmp_row': list})
        if not tmp.empty:
            tmp = tmp.reset_index()
            for _, row in tmp.iterrows():
                results_args.update({
                    "badrows": row.tmp_row, # list of rows associated with the batch that doesnt have a sample dup
                    "badcolumn": "SampleType",
                    "error_type": "Incomplete data",
                    "error_message": f"The batch {row.analysisbatchid} is missing a sample duplicate for {row.analytename}"
                })
                warnings.append(checkData(**results_args))
    # --- END Table 5-6 Check #4 --- #

    # --- TABLE 5-6 Check #5 --- #
    # Check - Duplicate Results must have RPD < 30% (WARNING)
    print('# Check - Duplicate Results must have RPD < 30% (WARNING)')
    checkdf = results[results.analyteclass.isin(['TOC','TN']) & (results.sampletype == 'Result')]
    if not checkdf.empty:
        checkdf = checkdf.groupby(['analysisbatchid', 'analytename','sampleid']).apply(
            lambda subdf:
            abs((subdf.result.max() - subdf.result.min()) / ((subdf.result.max() + subdf.result.min()) / 2))
        )
        if not checkdf.empty:
            #checkdf = checkdf.reset_index(name = 'rpd')
            checkdf = checkdf.reset_index()
            checkdf = checkdf.rename(columns = {0:'rpd'})
            print("checkdf has rpd column")
            print(checkdf)
  
            checkdf['errmsg'] = checkdf.apply(
                lambda row:
                f"Duplicate Matrix spikes should have an RPD under 30% (for TOC and TN)"
                , axis = 1
            )
            checkdf = results[results.analyteclass.isin(['TOC','TN']) & (results.sampletype == 'Result')] \
                .merge(
                    checkdf[
                        # just merge records that failed the check
                        checkdf.rpd.apply(lambda x: x >= .30)
                    ], 
                    on = ['analysisbatchid','analytename','sampleid'], 
                    how = 'inner'
                )
            if not checkdf.empty:
                argslist = checkdf.groupby(['errmsg']) \
                    .apply(lambda df: df.tmp_row.tolist()) \
                    .reset_index() \
                    .rename(columns = {0:'badrows'}) \
                    .apply(
                        lambda row: 
                        {
                            "badrows": row.badrows,
                            "badcolumn": "Result",
                            "error_type": "Value Error",
                            "error_message": row.errmsg
                        },
                        axis = 1
                    ).tolist()
                for args in argslist:
                    results_args.update(args)
                    warnings.append(checkData(**results_args))
    
    # --- END TABLE 5-6 Check #5 --- #



    # ------- END Table 5-6 - TOC and TN, Non-tissue matrices (Sediment and labwater) -------#
    print("# ------- END Table 5-6 - TOC and TN, Non-tissue matrices (Sediment and labwater) -------#")


    # -=======- END BIGHT CHEMISTRY QA PLAN CHECKS -=======- #  
    
    return {'errors': errs, 'warnings': warnings}
