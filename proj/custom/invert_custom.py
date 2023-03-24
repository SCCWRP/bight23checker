# Dont touch this file! This is intended to be a template for implementing new custom checks

from inspect import currentframe
from flask import current_app, g
from .functions import checkData, multivalue_lookup_check
from sqlalchemy import create_engine
import pandas as pd
import re


def invert(all_dfs):

    current_function_name = str(currentframe().f_code.co_name)

    # function should be named after the dataset in app.datasets in __init__.py
    assert current_function_name in current_app.datasets.keys(), \
        f"function {current_function_name} not found in current_app.datasets.keys() - naming convention not followed"

    expectedtables = set(current_app.datasets.get(
        current_function_name).get('tables'))
    assert expectedtables.issubset(set(all_dfs.keys())), \
        f"""In function {current_function_name} - {expectedtables - set(all_dfs.keys())} not found in keys of all_dfs ({','.join(all_dfs.keys())})"""

    # define errors and warnings list
    errs = []
    warnings = []

    trawlinvertebrateabundance = all_dfs['tbl_trawlinvertebrateabundance']
    trawlinvertebratebiomass = all_dfs['tbl_trawlinvertebratebiomass']

    trawlinvertebrateabundance = trawlinvertebrateabundance.assign(tmp_row = trawlinvertebrateabundance.index)
    trawlinvertebratebiomass = trawlinvertebratebiomass.assign(tmp_row = trawlinvertebratebiomass.index)

    trawlinvertebrateabundance_args = {
        "dataframe": trawlinvertebrateabundance,
        "tablename": 'tbl_trawlinvertebrateabundance',
        "badrows": [],
        "badcolumn": "",
        "error_type": "",
        "is_core_error": False,
        "error_message": ""
    }

    trawlinvertebratebiomass_args = {
        "dataframe": trawlinvertebratebiomass,
        "tablename": 'tbl_trawlinvertebratebiomass',
        "badrows": [],
        "badcolumn": "",
        "error_type": "",
        "is_core_error": False,
        "error_message": ""
    }

    # STARTING CHECKS

    ## LOGIC ##
    print("Starting Invert Logic Checks")
    # Jordan - Each invertebrate abundance/biomass record must have a corresponding trawl assemblage event record and each trawl assemblage event record must have must have a corresponding invertebrate abundance/biomass record. [records are matched on StationID, SampleDate, Sampling Organization, and Trawl Number]
    print('Each invertebrate abundance/biomass record must have a corresponding trawl assemblage event record and each trawl assemblage event record must have must have a corresponding invertebrate abundance/biomass record. [records are matched on StationID, SampleDate, Sampling Organization, and Trawl Number]')

    # call database for trawl assemblage data.
    eng = g.eng
    ta_db = eng.execute("SELECT stationid,sampledate,samplingorganization,trawlnumber FROM tbl_trawlevent;")
    ta = pd.DataFrame(ta_db.fetchall())
    if len(ta) > 0:
        ta.columns = ta_db.keys()
        # Series containing pertinent trawl assemblage and invert abundance/biomass records
        trawl_assemblage = zip(
            ta.stationid, 
            ta.sampledate,
            ta.samplingorganization, 
            ta.trawlnumber
        )
        invert_ab = pd.Series(
            zip(
                trawlinvertebrateabundance.stationid, 
                trawlinvertebrateabundance.sampledate,
                trawlinvertebrateabundance.samplingorganization, 
                trawlinvertebrateabundance.trawlnumber
            )
        )
        invert_bio = pd.Series(
            zip(
                trawlinvertebratebiomass.stationid, 
                trawlinvertebratebiomass.sampledate,
                trawlinvertebratebiomass.samplingorganization, 
                trawlinvertebratebiomass.trawlnumber
            )
        )

        # Check To see if there is any data in invert trawlinvertebrateabundance, not in trawl assemblage and vice versa
        log_error1 = trawlinvertebrateabundance.loc[
            ~invert_ab.apply(
                lambda x: x in trawl_assemblage
            )
        ]
        print(log_error1)
        badrows = log_error1.tmp_row.tolist()
        trawlinvertebrateabundance_args = {
            "dataframe": trawlinvertebrateabundance,
            "tablename": 'tbl_trawlinvertebrateabundance',
            "badrows": badrows,
            "badcolumn": "stationid,sampledate,samplingorganization,trawlnumber",
            "error_type": "Logic Error",
            "is_core_error": False,
            "error_message": "Each invertebrate abundance record must have a corresponding trawl assemblage event record. Records are matched on StationID, SampleDate, SamplingOrganiztion and TrawlNumber."
        }
        errs = [*errs, checkData(**trawlinvertebrateabundance_args)]

        log_error2 = trawlinvertebratebiomass.loc[
            ~invert_bio.apply(
                lambda x: x in trawl_assemblage
            )
        ]
        print(log_error2)
        
        badrows = log_error2.tmp_row.tolist()
        trawlinvertebratebiomass_args = {
            "dataframe": trawlinvertebratebiomass,
            "tablename": 'tbl_trawlinvertebratebiomass',
            "badrows": badrows,
            "badcolumn": "stationid,sampledate,samplingorganization,trawlnumber",
            "error_type": "Logic Error",
            "is_core_error": False,
            "error_message": "Each invertebrate biomass record must have a corresponding trawl assemblage event record. Records are matched on StationID, SampleDate, SamplingOrganization and TrawlNumber."
        }
        errs = [*errs, checkData(**trawlinvertebratebiomass_args)]

    else:

        badrows = trawlinvertebrateabundance.tmp_row.tolist()
        trawlinvertebrateabundance_args = {
            "dataframe": trawlinvertebrateabundance,
            "tablename": 'tbl_trawlinvertebrateabundance',
            "badrows": badrows,
            "badcolumn": "stationid",
            "error_type": "Abundance Error",
            "is_core_error": False,
            "error_message": "Table is Empty."
        }
        errs = [*errs, checkData(**trawlinvertebrateabundance_args)]

        badrows = trawlinvertebratebiomass.tmp_row.tolist()
        trawlinvertebratebiomass_args = {
            "dataframe": trawlinvertebratebiomass,
            "tablename": 'tbl_trawlinvertebratebiomass',
            "badrows": badrows,
            "badcolumn": "stationid",
            "error_type": "Biomass Error",
            "is_core_error": False,
            "error_message": "Table is Empty."
        }
        errs = [*errs, checkData(**trawlinvertebratebiomass_args)]
    ## END LOGIC CHECKS ##
    print("## END LOGIC CHECKS ##")

    ## CUSTOM CHECKS ##
    ############################
    # ABUNDANCE/BIOMASS CHECKS #
    ############################
    print("## CUSTOM CHECKS ##")
    print("# ABUNDANCE/BIOMASS CHECKS #")
    # Jordan - Species Check -

    # Duy: The below function replaces dcValueAgainstMultipleValues
    # Jordan - Anomaly Check - A single anomaly is required but multiple anomalies are possible (many to many).
    tmpargs = multivalue_lookup_check(
        trawlinvertebrateabundance,
        'anomaly',
        'lu_invertanomalies',
        'anomaly',
        eng,
        displayfieldname="Anomaly"
    )
    trawlinvertebrateabundance_args.update(tmpargs)
    errs = [*errs, checkData(**trawlinvertebrateabundance_args)]

    # Jordan - QA Check - A single qualifier is required but multiple qualifiers are possible (many to many).
    print("QA Check - A single qualifier is required but multiple qualifiers are possible (many to many).")
    tmpargs = multivalue_lookup_check(
        trawlinvertebrateabundance,
        'abundancequalifier',
        'lu_trawlqualifier',
        'qualifier',
        eng,
        displayfieldname="AbundanceQualifier"
    )
    trawlinvertebrateabundance_args.update(tmpargs)
    errs = [*errs, checkData(**trawlinvertebrateabundance_args)]

    tmpargs = multivalue_lookup_check(
        trawlinvertebratebiomass,
        'biomassqualifier',
        'lu_trawlqualifier',
        'qualifier',
        eng,
        displayfieldname="BiomassQualifier"
    )
    trawlinvertebratebiomass_args.update(tmpargs)
    errs = [*errs, checkData(**trawlinvertebratebiomass_args)]

    

    # Jordan - Species - Check Southern California Association of Marine Invertebrate Taxonomists Edition 12 - Check old species name
    print("Species - Check Southern California Association of Marine Invertebrate Taxonomists Edition 12 - Check old species name")
    spcs_names = eng.execute("SELECT synonym, taxon FROM lu_invertsynonyms;")
    sn = pd.DataFrame(spcs_names.fetchall()); sn.columns = spcs_names.keys()

    badrows = trawlinvertebrateabundance[
        trawlinvertebrateabundance.invertspecies.isin(sn.synonym.tolist())
    ].tmp_row.tolist()
    trawlinvertebrateabundance_args = {
        "dataframe": trawlinvertebrateabundance,
        "tablename": 'tbl_trawlinvertebrateabundance',
        "badrows": badrows,
        "badcolumn": "invertspecies",
        "error_type": "Undefined Warning",
        "is_core_error": False,
        "error_message":
            f'The species you entered is possibly a synonym. Please verify by checking the lookup list: <a href=/{current_app.script_root}/scraper?action=help&layer=lu_invertsynonyms target=_blank>lu_invertsynonyms</a>'
    }
    warnings = [*warnings, checkData(**trawlinvertebrateabundance_args)]

    badrows = trawlinvertebratebiomass[
        trawlinvertebratebiomass.invertspecies.isin(sn.synonym.tolist())
    ].tmp_row.tolist()
    trawlinvertebratebiomass_args = {
        "dataframe": trawlinvertebratebiomass,
        "tablename": 'tbl_trawlinvertebratebiomass',
        "badrows": badrows,
        "badcolumn": "invertspecies",
        "error_type": "Undefined Warning",
        "is_core_error": False,
        "error_message":
            f'The species you entered is possibly a synonym. Please verify by checking the lookup list: <a href=/{current_app.script_root}/scraper?action=help&layer=lu_invertsynonyms target=_blank>lu_invertsynonyms</a>'
    }
    warnings = [*warnings, checkData(**trawlinvertebratebiomass_args)]


    # NOTE: "Composite Weight records are no longer going to be submitted for fish or invert." - Shelly 9/12/2018
    # Jordan - Cross table checks - abundance vs. biomass  Link both abundance and biomass submissions and run mismatch query to check for orphan records. "Composite weight" should be only mismatch.  Error message - Orphan records for biomass vs abundance.
    print('Cross table checks - abundance vs. biomass Link both abundance and biomass submissions and run mismatch query to check for orphan records.')
    badrows = trawlinvertebratebiomass[
        ~trawlinvertebratebiomass[['stationid', 'sampledate', 'samplingorganization', 'trawlnumber', 'invertspecies']].isin(
            trawlinvertebrateabundance[['stationid', 'sampledate', 'samplingorganization', 'trawlnumber', 'invertspecies']].to_dict(orient='list')
        ).all(axis=1)
    ].tmp_row.tolist()
    print(badrows)
    trawlinvertebratebiomass_args = {
        "dataframe": trawlinvertebratebiomass,
        "tablename": 'tbl_trawlinvertebratebiomass',
        "badrows": badrows,
        "badcolumn": "stationid,sampledate,samplingorganization,invertspecies",
        "error_type": "Biomass Error",
        "is_core_error": False,
        "error_message": "Orphan records for biomass vs. abundance."
    }
    errs = [*errs, checkData(**trawlinvertebratebiomass_args)]

    print('finish cross table check')
    ## END ABUNDANCE/BIOMASS CHECKS ##
    print("## END ABUNDANCE/BIOMASS CHECKS ##")

    
    
    
    #########################
    # ABUNDANCE ONLY CHECKS #
    #########################
    print("# ABUNDANCE ONLY CHECKS #")

    # Jordan - Range check - Check depth ranges (min & max) for all species.
    print("Range check - Check depth ranges (min & max) for all species.")
    # 1st. Get StartDepth and EndDepth for each unique StationID/SampleDate/SamplingOrganization record from tbl_trawlevent
    trawl_depths = eng.execute(
        "SELECT stationid,sampledate,samplingorganization,trawlnumber,startdepth,enddepth FROM tbl_trawlevent;"
    )
    td = pd.DataFrame(trawl_depths.fetchall())
    td.columns = trawl_depths.keys()
    # 2nd. Get Min and Max Depths for each Species from lu_invertspeciesdepthrange
    lu_depthranges = eng.execute(
        "SELECT species AS invertspecies,mindepth,maxdepth FROM lu_invertspeciesdepthrange;"
    )
    depth_ranges = pd.DataFrame(lu_depthranges.fetchall())
    depth_ranges.columns = lu_depthranges.keys()
    # 3rd. Merge Trawl Depth Records and Submitted Invertebrate Abundance Records on StationID/SampleDate/SamplingOrganization
    tam = trawlinvertebrateabundance[
        ['stationid', 'sampledate', 'samplingorganization', 'trawlnumber', 'invertspecies', 'tmp_row']
    ]\
        .merge(
            depth_ranges, 
            on='invertspecies'
        ).merge(
            td, 
            on=['stationid', 'sampledate', 'samplingorganization', 'trawlnumber']
        )

    if not tam.empty:
        tam['inrange'] = tam.apply(
            lambda x: 
            False if (max(x.startdepth, x.enddepth) < x.mindepth) | (min(x.startdepth, x.enddepth) > x.maxdepth) else True, axis=1
        )
        badrecords = tam[tam.inrange == False]
        
        for i, row in badrecords.iterrows():
            
            trawlinvertebrateabundance_args = {
                "dataframe": trawlinvertebrateabundance,
                "tablename": 'tbl_trawlinvertebrateabundance',
                "badrows": [row.tmp_row],
                "badcolumn": "invertspecies",
                "error_type": "Undefined Warning",
                "is_core_error": False,
                "error_message":
                    '%s was caught in a depth range (%sm - %sm) that does not include the range it is typically found (%sm - %sm). Please verify the species is correct. Check <a href=/%s/scraper?action=help&layer=lu_invertspeciesdepthrange target=_blank>lu_invertspeciesdepthrange</a> for more information.' % (tam.invertspecies[i], int(tam.startdepth[i]), int(tam.enddepth[i]), tam.mindepth[i], tam.maxdepth[i], current_app.script_root)
            }
            warnings = [*warnings, checkData(**trawlinvertebrateabundance_args)]
        print("done with for loop")
    # Jordan - Species - Check list of non-trawl taxa (next tab)
    invalid_species = eng.execute("SELECT species AS invertspecies FROM lu_invertspeciesnotallowed;")
    invs = pd.DataFrame(invalid_species.fetchall()); invs.columns= invalid_species.keys()
    badrows = trawlinvertebrateabundance[trawlinvertebrateabundance.invertspecies.isin(invs.invertspecies.tolist())].tmp_row.tolist()
    trawlinvertebrateabundance_args = {
        "dataframe": trawlinvertebrateabundance,
        "tablename": 'tbl_trawlinvertebrateabundance',
        "badrows": badrows,
        "badcolumn": "invertspecies",
        "error_type": "Undefined Error",
        "is_core_error": False,
        "error_message":
            f'Holoplanktonic or infaunal species. See lookup list: <a href=/{current_app.script_root}/scraper?action=help&layer=lu_invertspeciesnotallowed target=_blank>lu_invertspeciesnotallowed</a>'
    }
    errs = [*errs, checkData(**trawlinvertebrateabundance_args)]

    # Jordan - Anomaly - If Anomaly = Other, a comment is required.
    # print('If Anomaly = Other, a comment is required.')
    missing_comments = trawlinvertebrateabundance[
        (trawlinvertebrateabundance.anomaly == 'Other') & 
        ((trawlinvertebrateabundance.comments == '') | (trawlinvertebrateabundance.comments.isnull()))
    ]
    print(missing_comments)
    badrows = missing_comments.tmp_row.tolist()
    trawlinvertebrateabundance_args = {
        "dataframe": trawlinvertebrateabundance,
        "tablename": 'tbl_trawlinvertebrateabundance',
        "badrows": badrows,
        "badcolumn": "anomaly,comment",
        "error_type": "Undefined Error",
        "is_core_error": False,
        "error_message":
            'A comment is required for all anomalies listed as Other.'
    }
    errs = [*errs, checkData(**trawlinvertebrateabundance_args)]
    
    
    # Jordan - Anomaly - Check for single records that contain anomalies.
    print('Anomaly - Check for single records that contain anomalies.')
    single_records = trawlinvertebrateabundance[
        ~trawlinvertebrateabundance.duplicated(
            subset = ['stationid', 'sampledate', 'samplingorganization', 'trawlnumber','invertspecies'],
            keep=False
        )
    ]
    print(single_records[single_records.anomaly != 'None'])
    badrows = single_records[single_records.anomaly != 'None'].tmp_row.tolist()
    trawlinvertebrateabundance_args = {
        "dataframe": trawlinvertebrateabundance,
        "tablename": 'tbl_trawlinvertebrateabundance',
        "badrows": badrows,
        "badcolumn": "anomaly",
        "error_type": "Undefined Warning",
        "is_core_error": False,
        "error_message":
            'Anomalies and clean organisms may be lumped together( e.g. 128 urchins, all with parasites isnt likely'
    }
    warnings = [*warnings, checkData(**trawlinvertebrateabundance_args)]
    ## END ABUNDANCE ONLY CHECKS ##
    print("## END ABUNDANCE ONLY CHECKS ##")



    #######################
    # BIOMASS ONLY CHECKS #
    #######################

    # td = trawl depths - defined in beginning of abundance section
    tbm = trawlinvertebratebiomass[
        ['stationid', 'sampledate', 'samplingorganization', 'trawlnumber', 'invertspecies', 'tmp_row']
    ]\
        .merge(
            depth_ranges, 
            on='invertspecies'
        ).merge(
            td, 
            on=['stationid', 'sampledate', 'samplingorganization', 'trawlnumber']
        )

    if not tbm.empty:
        tbm['inrange'] = tbm.apply(
            lambda x: 
            False if (max(x.startdepth, x.enddepth) < x.mindepth) | (min(x.startdepth, x.enddepth) > x.maxdepth) else True, axis=1
        )
        badrecords = tbm[tbm.inrange == False]
        
        for i, row in badrecords.iterrows():
            
            trawlinvertebratebiomass_args = {
                "dataframe": trawlinvertebratebiomass,
                "tablename": 'tbl_trawlinvertebratebiomass',
                "badrows": [row.tmp_row],
                "badcolumn": "invertspecies",
                "error_type": "Undefined Warning",
                "is_core_error": False,
                "error_message":
                    '%s was caught in a depth range (%sm - %sm) that does not include the range it is typically found (%sm - %sm). Please verify the species is correct. Check <a href=/%s/scraper?action=help&layer=lu_invertspeciesdepthrange target=_blank>lu_invertspeciesdepthrange</a> for more information.' % (tam.invertspecies[i], int(tam.startdepth[i]), int(tam.enddepth[i]), tam.mindepth[i], tam.maxdepth[i], current_app.script_root)
            }
            warnings = [*warnings, checkData(**trawlinvertebratebiomass_args)]
        print("done with for loop")



    print("# BIOMASS ONLY CHECKS #")
    print("Kristin - Check data to make sure minimum weight is not less than value of <0.1 kg")
    print(trawlinvertebratebiomass[trawlinvertebratebiomass.biomass == 0])
    badrows = trawlinvertebratebiomass[trawlinvertebratebiomass.biomass == 0].tmp_row.tolist()
    trawlinvertebratebiomass_args = {
        "dataframe": trawlinvertebratebiomass,
        "tablename": 'tbl_trawlinvertebratebiomass',
        "badrows": badrows,
        "badcolumn": "biomass",
        "error_type": "Undefined Error",
        "is_core_error": False,
        "error_message":
            'Weight submitted as 0 should have been <0.1kg'
    }
    errs = [*errs, checkData(**trawlinvertebratebiomass_args)]  

    #NOTE: the following check is alot more tricky than expected due to the way pandas (or possible Excel?) handles floats. -Jordan 9/12/18
    # Function provides us a way to strip trailing 0's from floats. 
    # def format_number(num):
    #     try:
    #         dec = decimal.Decimal(num)
    #     except:
    #         return 'bad'
    #     tup = dec.as_tuple()
    #     delta = len(tup.digits) + tup.exponent
    #     digits = ''.join(str(d) for d in tup.digits)
    #     if delta <= 0:
    #         zeros = abs(tup.exponent) - len(tup.digits)
    #         val = '0.' + ('0'*zeros) + digits
    #     else:
    #         val = digits[:delta] + ('0'*tup.exponent) + '.' + digits[delta:]
    #     val = val.rstrip('0')
    #     if val[-1] == '.':
    #         val = val[:-1]
    #     if tup.sign:
    #         return '-' + val
    #     return val
    print("Kristin - If biomass was measured with greater resolution than what is required in the IM plan ( only one decimal place is allowed), data should be rounded to the nearest 0.1")
    #Rounding biomass to the nearest 0.1
    trawlinvertebratebiomass['biomass'] = [round(trawlinvertebratebiomass['biomass'][x], 1) for x in trawlinvertebratebiomass.index.tolist()]
    print(trawlinvertebratebiomass[(trawlinvertebratebiomass['biomass'] <.1)&~(trawlinvertebratebiomass['biomassqualifier'].isin(['<']))])
    
    badrows = trawlinvertebratebiomass[
        (trawlinvertebratebiomass['biomass'] < .1) 
    ].index.tolist()
    
    trawlinvertebratebiomass_args = {
        "dataframe": trawlinvertebratebiomass,
        "tablename": 'tbl_trawlinvertebratebiomass',
        "badrows": badrows,
        "badcolumn": "biomass",
        "error_type": "Undefined Error",
        "is_core_error": False,
        "error_message":
            'Biomass values that were less than 0.1 kg (e.g. 0.004 kg) should have been submitted as <0.1 kg'
    }
    errs = [*errs, checkData(**trawlinvertebratebiomass_args)]  

    #Jordan - Biomass - Filter qualifiers to make sure that all < have corresponding values of 0.1
    print('Biomass - Filter qualifiers to make sure that all < have corresponding values of 0.1')
    badrows = trawlinvertebratebiomass[
        (trawlinvertebratebiomass.biomassqualifier == '<') & 
        (trawlinvertebratebiomass.biomass != 0.1)
    ].tmp_row.tolist()
    trawlinvertebratebiomass_args = {
        "dataframe": trawlinvertebratebiomass,
        "tablename": 'tbl_trawlinvertebratebiomass',
        "badrows": badrows,
        "badcolumn": "biomass",
        "error_type": "Undefined Error",
        "is_core_error": False,
        "error_message":
            'Less than qualifiers must have corresponding biomass value of 0.1kg.'
    }
    errs = [*errs, checkData(**trawlinvertebratebiomass_args)] 

    # Jordan said the below is no longer necessary, but that only applies to the year 2018
    # Need to make sure if it's necessary for 2023, so I am going to leave the below code commented out in case we need it - Duy
    '''
    NOTE: The following 4 checks are no longer necessary because composite weights will not be submitted. -Jordan 9/12/2018

    #Jordan - Biomass - Check to make sure that all "<0.1 kg" records have corresponding "Composite Weight" totals.
    lt = biomass[(biomass.biomassqualifier == '<')&(biomass.biomass == 0.1)&(biomass.invertspecies.str.lower() != 'compositewt')]
    cp = biomass[(biomass.invertspecies.str.lower() == 'compositewt')]
    errorLog('Biomass - Check to make sure that all <0.1 kg records have corresponding Composite Weight totals.')
    checkData(biomass[biomass.stationid.isin(set(lt.stationid)-set(cp.stationid))].tmp_row.tolist(),'Species/BiomassQualifier/Biomass','Undefined Error','error','<0.1 kg records submitted but not accompanying composite weight record.',biomass)
    #Jordan - Biomass - Check to see that each Composite weight has a corresponding record of "<0.1 kg".
    errorLog('Check to see that each Composite weight has a corresponding record of <0.1 kg.')
    checkData(biomass[biomass.stationid.isin(set(cp.stationid)-set(lt.stationid))].tmp_row.tolist(),'Species/BiomassQualifier/Biomass','Undefined Error','error','Composite weight submitted, but no accompanying <0.1 kg records for that station.',biomass)
    #Jordan - Biomass - Compare "<0.1 kg" records with "Composite Weight" records to make sure they make sense.
    errorLog('Compare <0.1 kg records with Composite Weight records to make sure they make sense.')
    single_lt_stations = lt.groupby('stationid').size().reset_index()[lt.groupby('stationid').size().reset_index()[0]==1].stationid.tolist()
    bm = biomass[(biomass.stationid.isin(single_lt_stations))&(biomass.invertspecies.str.lower() == 'compositewt')&(biomass.biomass > 0.1)]
    checkData(bm.tmp_row.tolist(),'Species/BiomassQualifier/Biomass','Undefined Error','error','Only one <0.1 kg record submitted but accompanying composite weight is more than 0.1 kg.',biomass)

    #Jordan/Kristin - Cross table checks - abundance vs. biomass  Check to make sure that total amount of records in biomass table is one more than abundance table. If not, make sure the reason makes sense.
    #Get list of different stations in df
    stid = abundance.stationid.unique()
    for station in stid:
        if abs(len(biomass[biomass['stationid'] == station].invertspecies.unique()) - len(abundance[abundance['stationid'] == station].invertspecies.unique())) > 1 :
            checkData(biomass[biomass['stationid'] == station].index.tolist(), 'InvertSpecies','biomass error','error','Biomass records were either too great or too small compared to abundance records',biomass)
    '''
    ## END BIOMASS ONLY CHECKS ##
    ## END CUSTOM CHECKS ##
    print("## END BIOMASS ONLY CHECKS ##")
    print("## END CUSTOM CHECKS ##")
        
    return {'errors': errs, 'warnings': warnings}
