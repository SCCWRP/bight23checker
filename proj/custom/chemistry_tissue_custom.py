# Dont touch this file! This is intended to be a template for implementing new custom checks

from inspect import currentframe
from flask import current_app, g
from datetime import timedelta
from .functions import checkData, checkLogic, sample_assignment_check
from .chem_functions_custom import *
import pandas as pd
import re

def chemistry_tissue(all_dfs):
    
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

    # Calculate percent recovery
    results['percentrecovery'] = \
        results.apply(
            lambda x: 
            float(x.result)/float(x.truevalue)*100 if ('spike' in x.sampletype.lower())|('reference' in x.sampletype.lower()) else -88, 
            axis = 1
        )


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
    # checkLogic function will update the arguments
    # Check for records in batch but not results
    print("batch")
    print(batch)
    print("results")
    print(results)
    batch_args.update(
        checkLogic(batch, results, ['lab','preparationbatchid'], df1_name = 'Batch', df2_name = 'Results')
    )
    errs.append(checkData(**batch_args))

    # Check for records in results but not batch
    results_args.update(
        checkLogic(results, batch, ['lab','preparationbatchid'], df1_name = 'Results', df2_name = 'Batch')
    )
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
    errs.append(checkData(**results_args))

    # Check - if the qualifier is "estimated" or "below reporting level" then the result must be between the mdl and rl (inclusive) (Error)
    print('# Check - if the qualifier is "estimated" or "below reporting level" then the result must be between the mdl and rl (inclusive) (Error)')
    results_args.update({
        "badrows": results[
                (results.qualifier.isin(["estimated", "below reporting level"])) & ((results.result < results.mdl) | (results.result > results.rl))
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

    # Check - if the qualifier is "none" then the result must be greater than the RL (Error)
    print('# Check - if the qualifier is "none" then the result must be greater than the RL (Error)')
    results_args.update({
        "badrows": results[(results.qualifier == 'none') & (results.result <= results.rl)].tmp_row.tolist(),
        "badcolumn": "Qualifier, Result",
        "error_type": "Value Error",
        "error_message": "if the qualifier is 'none' then the result must be greater than the RL"
    })
    errs.append(checkData(**results_args))

    # Check - Comment is required if the qualifier says "analyst error" "contaminated" or "interference" (Error)
    print('# Check - Comment is required if the qualifier says "analyst error" "contaminated" or "interference" (Error)')
    results_args.update({
        "badrows": results[(results.qualifier.isin(["analyst error","contaminated","interference"])) & (results.fillna('').comments == '')].tmp_row.tolist(),
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
        "error_message": "We would like to be contacted concerning this record of data. Please contact bight23-im@sccwrp.org"
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
        "Inorganics": ['Method blank', 'Blank spiked', 'Result'],
        "PAH": ['Method blank', 'Matrix spike', 'Result'],
        "PCB": ['Method blank', 'Matrix spike', 'Result'],
        "Chlorinated Hydrocarbons": ['Method blank', 'Matrix spike', 'Result'],
        "PBDE": ['Method blank', 'Matrix spike', 'Result'],
        "Pyrethroid": ['Method blank', 'Matrix spike', 'Result'],
        "FIPRONIL" : ['Method blank', 'Matrix spike', 'Result'],
        "TN" : ['Method blank', 'Result'],
        "TOC" : ['Method blank', 'Result']
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

    
    requires_crm = ["Inorganics", "PAH", "PCB", "Chlorinated Hydrocarbons", "PBDE", "TOC"]
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

    # Check - For sampletype Method blank, if Result is less than MDL, it must be -88
    print('# Check - For sampletype Method blank, if Result is less than MDL, it must be -88')
    # mb_mask = Method blank mask
    print('# mb_mask = Method blank mask')
    mb_mask = (results.sampletype == 'Method blank') 
    results_args.update({
        "badrows": results[mb_mask & ((results.result < results.mdl) & (results.result != -88))].tmp_row.tolist(),
        "badcolumn": "Result",
        "error_type": "Value Error",
        "error_message": "For Method blank sampletypes, if Result is less than MDL, it must be -88"
    })
    errs.append(checkData(**results_args))

    # Check - If SampleType=Method blank and Result=-88, then qualifier must be below MDL or none.
    print('# Check - If SampleType=Method blank and Result=-88, then qualifier must be below MDL or none.')
    results_args.update({
        "badrows": results[(mb_mask & (results.result != -88)) & (~results.qualifier.isin(['below method detection limit','none'])) ].tmp_row.tolist(),
        "badcolumn": "Qualifier",
        "error_type": "Value Error",
        "error_message": "If SampleType=Method blank and Result=-88, then qualifier must be 'below method detection limit' or 'none'"
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
    


    # ------------------------------------------------------------------------------------------------------------#
    print('# ------------------------------------------------------------------------------------------------------------#')
    # Check - For analyteclass Pyrethroid - within the same analysisbatch, you cant have both:
    print('# Check - For analyteclass Pyrethroid - within the same analysisbatch, you cant have both:')
    # 1. "Deltamethrin/Tralomethrin" and "Deltamethrin"
    print('# 1. "Deltamethrin/Tralomethrin" and "Deltamethrin"')
    # 2. "Esfenvalerate/Fenvalerate" and "Esfenvalerate"
    print('# 2. "Esfenvalerate/Fenvalerate" and "Esfenvalerate"')
    # 3. "Permethrin, cis" and "Permethrin (cis/trans)"
    print('# 3. "Permethrin, cis" and "Permethrin (cis/trans)"')
    # 4. "Permethrin, trans" and "Permethrin (cis/trans)"
    print('# 4. "Permethrin, trans" and "Permethrin (cis/trans)"')

    results_args.update(pyrethroid_analyte_logic_check(results, ["Deltamethrin/Tralomethrin", "Deltamethrin"]))
    errs.append(checkData(**results_args))
    results_args.update(pyrethroid_analyte_logic_check(results, ["Esfenvalerate/Fenvalerate", "Esfenvalerate"]))
    errs.append(checkData(**results_args))
    results_args.update(pyrethroid_analyte_logic_check(results, ["Permethrin, cis", "Permethrin (cis/trans)"]))
    errs.append(checkData(**results_args))
    results_args.update(pyrethroid_analyte_logic_check(results, ["Permethrin, trans", "Permethrin (cis/trans)"]))
    errs.append(checkData(**results_args))
    
    # END Check - For analyteclass Pyrethroid - within the same analysisbatch, you cant have both .......
    print('# END Check - For analyteclass Pyrethroid - within the same analysisbatch, you cant have both .......')
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
    # Check - for FIPRONIL and Pyrethroid, in the sediment matrix, the units must be ng/g dw
    print('# Check - for FIPRONIL and Pyrethroid, in the sediment matrix, the units must be ng/g dw')
    fip_pyre_mask = ((results.matrix == 'sediment') & (results.analyteclass.isin(['FIPRONIL','Pyrethroid']))) & (results.units != 'ng/g dw')
    
    results_args.update({
        "badrows": results[fip_pyre_mask].tmp_row.tolist(),
        "badcolumn": "Units",
        "error_type": "Value Error",
        "error_message": f"for FIPRONIL and Pyrethroid (where matrix = sediment), the units must be ng/g dw"
    })
    # -----------------------------------------------------------------------------------------------------------------------------------

    # ----- END CUSTOM CHECKS - SEDIMENT RESULTS ----- #

    # If there are errors, dont waste time with the QA plan checks
    # For testing, let us not enforce this, or we will waste a lot of time cleaning data
    # if errs != []:
    #     return {'errors': errs, 'warnings': warnings}



    # -=======- BIGHT CHEMISTRY QA PLAN CHECKS -=======- #  

    # ------- Table 6-2 - Inorganics (Mercury, Selenium, Arsenic), tissue matrices (tissue and labwater) -------#
    # ------- END Table 6-2 - Inorganics (Mercury, Selenium, Arsenic), tissue matrices (tissue and labwater) -------#


    # ------- Table 6-3 - PCBs, Chlorinated Pesticides, and PFAS, tissue matrices (tissue and labwater) -------#
    # ------- END Table 6-3 - PCBs, Chlorinated Pesticides, and PFAS, tissue matrices (tissue and labwater) -------#
    
    return {'errors': errs, 'warnings': warnings}
