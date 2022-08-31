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

    trawlinvertebrateabundance = all_dfs['tbl_trawlinvertebrateabundance']
    trawlinvertebratebiomass = all_dfs['tbl_trawlinvertebratebiomass']


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
    
    ### STARTING CHECKS

    ## LOGIC ##
    print("Starting Invert Logic Checks")
    #Jordan - Each invertebrate abundance/biomass record must have a corresponding trawl assemblage event record and each trawl assemblage event record must have must have a corresponding invertebrate abundance/biomass record. [records are matched on StationID, SampleDate, Sampling Organization, and Trawl Number]
    print('Each invertebrate abundance/biomass record must have a corresponding trawl assemblage event record and each trawl assemblage event record must have must have a corresponding invertebrate abundance/biomass record. [records are matched on StationID, SampleDate, Sampling Organization, and Trawl Number]')

    # call database for trawl assemblage data.
    eng = g.eng
    ta_db = eng.execute("SELECT stationid,sampledate,samplingorganization,trawlnumber FROM tbl_trawlevent;")
    ta = pd.DataFrame(ta_db.fetchall())
    if len(ta)>0:
        ta.columns = ta_db.keys()
        # Series containing pertinent trawl assemblage and invert abundance/biomass records
        trawl_assemblage = zip(ta.stationid,ta.sampledate,ta.samplingorganization,ta.trawlnumber)
        invert_ab = pd.Series(zip(trawlinvertebrateabundance.stationid,trawlinvertebrateabundance.sampledate,trawlinvertebrateabundance.samplingorganization,trawlinvertebrateabundance.trawlnumber))
        invert_bio = pd.Series(zip(trawlinvertebratebiomass.stationid,trawlinvertebratebiomass.sampledate,trawlinvertebratebiomass.samplingorganization,trawlinvertebratebiomass.trawlnumber))

        # Check To see if there is any data in invert trawlinvertebrateabundance, not in trawl assemblage and vice versa
        log_error1 = trawlinvertebrateabundance.loc[~invert_ab.apply(lambda x: x in trawl_assemblage)]
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
        
        log_error2 = trawlinvertebratebiomass.loc[~invert_bio.apply(lambda x: x in trawl_assemblage)]
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
    
    ## CUSTOM CHECKS ##
    ############################
    # ABUNDANCE/BIOMASS CHECKS #
    ############################
    # Jordan - Species Check - 

    #Duy: The below function replaces dcValueAgainstMultipleValues
    #Jordan - Anomaly Check - A single anomaly is required but multiple anomalies are possible (many to many).
    tmpargs = multivalue_lookup_check(
        trawlinvertebrateabundance, 
        'anomaly', 
        'lu_invertanomalies', 
        'anomaly', 
        displayfieldname = "TestAcceptability"
    )
    trawlinvertebrateabundance_args.update(tmpargs)
    errs = [*errs, checkData(**trawlinvertebrateabundance_args)]   

    #Jordan - QA Check - A single qualifier is required but multiple qualifiers are possible (many to many).
    print("QA Check - A single qualifier is required but multiple qualifiers are possible (many to many).")
    tmpargs = multivalue_lookup_check(
        trawlinvertebrateabundance, 
        'abundancequalifier', 
        'lu_trawlqualifier', 
        'qualifier', 
        displayfieldname = "AbundanceQualifier"
    )
    trawlinvertebrateabundance_args.update(tmpargs)
    errs = [*errs, checkData(**trawlinvertebrateabundance_args)]   
    
    tmpargs = multivalue_lookup_check(
        trawlinvertebratebiomass, 
        'biomassqualifier', 
        'lu_trawlqualifier', 
            'qualifier', 
        displayfieldname = "BiomassQualifier"
    )
    trawlinvertebratebiomass_args.update(tmpargs)
    errs = [*errs, checkData(**trawlinvertebratebiomass_args)]
    
    #Jordan - Species - Check Southern California Association of Marine Invertebrate Taxonomists Edition 8 - Check species not found in this region or depth
    print("Species - Check Southern California Association of Marine Invertebrate Taxonomists Edition 8 - Check species not found in this region or depth.")
    
    print("1st. Get StartDepth and EndDepth for each unique StationID/SampleDate/SamplingOrganization record from tbl_trawlevent")
    trawl_depths = eng.execute("SELECT stationid,sampledate,samplingorganization,trawlnumber,startdepth,enddepth FROM tbl_trawlevent;")
    td = pd.DataFrame(trawl_depths.fetchall())
    print(td)
    td.columns = trawl_depths.keys()
    # Get Invertebrate Species Depth Ranges
    spcs_depth = eng.execute("select species AS invertspecies,depth,acceptablerange,resolution from lu_invertspeciesreplaceondepth;")
    sd = pd.DataFrame(spcs_depth.fetchall()); sd.columns = spcs_depth.keys()
    print(sd)
    
    
    # Abundance: Merge Invertebrate Species and Trawl Depth Records on StationID/SampleDate/SamplingOrganization/TrawlNumber
    tam = trawlinvertebrateabundance[
        ['stationid','sampledate','samplingorganization','trawlnumber','invertspecies','tmp_row']
    ]\
        .merge(
            sd,
            on='invertspecies'
        ).merge(
            td,
            on=['stationid','sampledate','samplingorganization','trawlnumber']
        )
    print(tam)

    if not tam.empty:
        # Abundance: Parse acceptablerange field to obtain lower and upper bounds for species depth
        print("Abundance: Parse acceptablerange field to obtain lower and upper bounds for species depth")
        tam['mindepth'] = tam.apply(lambda x: int(re.search('.*?(?=-)',x.acceptablerange).group(0)), axis=1)
        tam['maxdepth'] = tam.apply(lambda x: int(re.search('(?<=-).*?(?= m)',x.acceptablerange).group(0)), axis=1)
        # create boolean field 'inrange' that dictates whether a given species is found at the depth the trawl was completed
        tam['inrange'] = tam.apply(
            lambda x: 
            False 
            if (max(x.startdepth,x.enddepth)<x.mindepth)|(min(x.startdepth,x.enddepth)>x.maxdepth) 
            else True, 
            axis = 1
        )
        # provide warning if species is not found in the trawls depth range
        for i in range(len(tam)):
            if tam['inrange'][i] == False:
                badrows = tam.iloc[i].tmp_row.tolist()
                trawlinvertebrateabundance_args = {
                    "dataframe": trawlinvertebrateabundance,
                    "tablename": 'tbl_trawlinvertebrateabundance',
                    "badrows": badrows,
                    "badcolumn": "invertspecies",
                    "error_type": "Undefined Warning",
                    "is_core_error": False,
                    "error_message": 
                        '%s was caught in a depth range (%sm - %sm) that does not include the range it is typically found (%sm - %sm). Please verify the species is correct. Check <a href=http://checker.sccwrp.org/checker/scraper?action=help&layer=lu_invertspeciesreplaceondepth target=_blank>lu_invertspeciesreplaceondepth</a> for more information.' %(tam.invertspecies[i],int(tam.startdepth[i]),int(tam.enddepth[i]),tam.mindepth[i],tam.maxdepth[i])
                }    
                warnings = [*warnings, checkData(**trawlinvertebrateabundance_args)]              
                
    # Biomass: Merge Invertebrate Species and Trawl Depth Records on StationID/SampleDate/SamplingOrganization/TrawlNumber
    print("Biomass: Merge Invertebrate Species and Trawl Depth Records on StationID/SampleDate/SamplingOrganization/TrawlNumber")
    tbm = trawlinvertebratebiomass[
        ['stationid','sampledate','samplingorganization','trawlnumber','invertspecies','tmp_row']
    ]\
        .merge(
            sd,
            on='invertspecies'
        ).merge(
            td,
            on=['stationid','sampledate','samplingorganization','trawlnumber']
        )
    print(tbm)
    if not tbm.empty:
        # Biomass: Parse acceptablerange field to obtain lower and upper bounds for species depth
        tbm['mindepth'] = tbm.apply(lambda x: int(re.search('.*?(?=-)',x.acceptablerange).group(0)), axis=1)
        tbm['maxdepth'] = tbm.apply(lambda x: int(re.search('(?<=-).*?(?= m)',x.acceptablerange).group(0)), axis=1)
        # Biomass: create boolean field 'inrange' that dictates whether a given species is found at the depth the trawl was completed
        # create boolean field 'inrange' that dictates whether a given species is found at the depth the trawl was completed
        tbm['inrange'] = tbm.apply(lambda x: False if (max(x.startdepth,x.enddepth)<x.mindepth)|(min(x.startdepth,x.enddepth)>x.maxdepth) else True, axis = 1)
        #provide warning if species is not found in the trawls depth range
        for i in range(len(tbm)):
            if tbm['inrange'][i] == False:
                badrows = tbm.iloc[i].tmp_row.tolist()
                print(badrows)
                trawlinvertebratebiomass_args = {
                    "dataframe": trawlinvertebratebiomass,
                    "tablename": 'tbl_trawlinvertebratebiomass',
                    "badrows": badrows,
                    "badcolumn": "invertspecies",
                    "error_type": "Undefined Warning",
                    "is_core_error": False,
                    "error_message": 
                        '%s was caught in a depth range (%sm - %sm) that does not include the range it is typically found (%sm - %sm). Please verify the species is correct. Check <a href=http://checker.sccwrp.org/checker/scraper?action=help&layer=lu_invertspeciesreplaceondepth target=_blank>lu_invertspeciesreplaceondepth</a> for more information.' %(tbm.invertspecies[i],int(tbm.startdepth[i]),int(tbm.enddepth[i]),tbm.mindepth[i],tbm.maxdepth[i])
                }    
                warnings = [*warnings, checkData(**trawlinvertebratebiomass_args)]  

    #Jordan - Species - Check Southern California Association of Marine Invertebrate Taxonomists Edition 12 - Check old species name
    print("Species - Check Southern California Association of Marine Invertebrate Taxonomists Edition 12 - Check old species name")
    spcs_names = eng.execute("SELECT synonym, taxon FROM lu_invertsynonyms;")
    sn = pd.DataFrame(spcs_names.fetchall()); sn.columns = spcs_names.keys()
    
    badrows = trawlinvertebrateabundance[trawlinvertebrateabundance.invertspecies.isin(sn.synonym.tolist())].tmp_row.tolist()
    trawlinvertebrateabundance_args = {
        "dataframe": trawlinvertebrateabundance,
        "tablename": 'tbl_trawlinvertebrateabundance',
        "badrows": badrows,
        "badcolumn": "invertspecies",
        "error_type": "Undefined Warning",
        "is_core_error": False,
        "error_message": 
                'The species you entered is possibly a synonym. Please verify by checking the lookup list: <a href=http://checker.sccwrp.org/checker/scraper?action=help&layer=lu_invertsynonyms target=_blank>lu_invertsynonyms</a>'
    }    
    warnings = [*warnings, checkData(**trawlinvertebrateabundance_args)]    
    
    badrows = trawlinvertebratebiomass_args[trawlinvertebratebiomass_args.invertspecies.isin(sn.synonym.tolist())].tmp_row.tolist()
    trawlinvertebratebiomass_args = {
        "dataframe": trawlinvertebratebiomass_args,
        "tablename": 'tbl_trawlinvertebrateabundance',
        "badrows": badrows,
        "badcolumn": "invertspecies",
        "error_type": "Undefined Warning",
        "is_core_error": False,
        "error_message": 
                'The species you entered is possibly a synonym. Please verify by checking the lookup list: <a href=http://checker.sccwrp.org/checker/scraper?action=help&layer=lu_invertsynonyms target=_blank>lu_invertsynonyms</a>'
    }    
    warnings = [*warnings, checkData(**trawlinvertebratebiomass_args)]    

    '''
    NOTE: This check determines whether or not the stationID submitted can be found in the field assignment table. However, the logic checks already
            determine whether the stations are found in trawl event table. Since the stations in the trawl event table is a subset of the stations in
            the field assignment table, this check seems to be unnecessary. - Jordan 9/12/2018
    #Jordan - Lookup list - Link data to Bight station list to look for mismatched records - Error -> Stations not Bight stations
    errorLog("Lookup list - Link data to Bight station list to look for mismatched records - Error -> Stations not Bight stations")
    field_assignment_table = eng.execute("select stationid from field_assignment_table;")
    fat = DataFrame(field_assignment_table.fetchall()); fat.columns = field_assignment_table.keys()
    errorLog('field assignment table records')
    errorLog(fat)
    errorLog(abundance[~abundance.stationid.isin(fat.stationid.tolist())])
    checkData(abundance[~abundance.stationid.isin(fat.stationid.tolist())].tmp_row.tolist(),'StationID','Undefined Error','error','Stations not Bight stations.',abundance)
    errorLog(biomass[~biomass.stationid.isin(fat.stationid.tolist())])
    checkData(biomass[~biomass.stationid.isin(fat.stationid.tolist())].tmp_row.tolist(),'StationID','Undefined Error','error','Stations not Bight stations.',biomass)
    '''

    #NOTE: "Composite Weight records are no longer going to be submitted for fish or invert." - Shelly 9/12/2018
    #Jordan - Cross table checks - abundance vs. biomass  Link both abundance and biomass submissions and run mismatch query to check for orphan records. "Composite weight" should be only mismatch.  Error message - Orphan records for biomass vs abundance.
    print('Cross table checks - abundance vs. biomass Link both abundance and biomass submissions and run mismatch query to check for orphan records.')
    print(trawlinvertebratebiomass[~trawlinvertebratebiomass[['stationid','sampledate','samplingorganization','trawlnumber','invertspecies']].isin(trawlinvertebrateabundance[['stationid','sampledate','samplingorganization','trawlnumber','invertspecies']].to_dict(orient='list')).all(axis=1)])
    badrows = trawlinvertebratebiomass[
        ~trawlinvertebratebiomass[['stationid','sampledate','samplingorganization','trawlnumber','invertspecies']].isin(
            trawlinvertebrateabundance[['stationid','sampledate','samplingorganization','trawlnumber','invertspecies']].to_dict(orient='list')
        ).all(axis=1)].tmp_row.tolist()
    trawlinvertebratebiomass_args = {
        "dataframe": trawlinvertebratebiomass,
        "tablename": 'tbl_trawlinvertebratebiomass',
        "badrows": badrows,
        "badcolumn": "stationid,sampledate,samplingorganization,invertspecies",
        "error_type": "Biomass Error",
        "is_core_error": False,
        "error_message": "TOrphan records for biomass vs. abundance."
    }    
    errs = [*errs, checkData(**trawlinvertebratebiomass_args)]          
        
    print('finish cross table check')
    ## END ABUNDANCE/BIOMASS CHECKS ##

    #########################
    # ABUNDANCE ONLY CHECKS #
    #########################
    
    #Jordan - Range check - Check depth ranges (min & max) for all species.
    print("Range check - Check depth ranges (min & max) for all species.")
    # 1st. Get StartDepth and EndDepth for each unique StationID/SampleDate/SamplingOrganization record from tbl_trawlevent
    trawl_depths = eng.execute("SELECT stationid,sampledate,samplingorganization,trawlnumber,startdepth,enddepth FROM tbl_trawlevent;")
    td = pd.DataFrame(trawl_depths.fetchall())
    td.columns = trawl_depths.keys()
    # 2nd. Get Min and Max Depths for each Species from lu_invertspeciesdepthrange
    lu_depthranges = eng.execute("SELECT species AS invertspecies,mindepth,maxdepth FROM lu_invertspeciesdepthrange;")
    depth_ranges = pd.DataFrame(lu_depthranges.fetchall())
    depth_ranges.columns = lu_depthranges.keys()
    # 3rd. Merge Trawl Depth Records and Submitted Invertebrate Abundance Records on StationID/SampleDate/SamplingOrganization
    tam = trawlinvertebrateabundance[['stationid','sampledate','samplingorganization','trawlnumber','invertspecies','tmp_row']].merge(depth_ranges,on='invertspecies').merge(td,on=['stationid','sampledate','samplingorganization','trawlnumber'])
    if not tam.empty:
        tam['inrange'] = tam.apply(lambda x: False if (max(x.startdepth,x.enddepth)<x.mindepth)|(min(x.startdepth,x.enddepth)>x.maxdepth) else True, axis = 1)
        for i in range(len(tam)):
            if tam['inrange'][i] == False:
                badrows = [tam.iloc[i].tmp_row],
                'InvertSpecies','Undefined Warning','warning','%s was caught in a depth range (%sm - %sm) that does not include the range it is typically found (%sm - %sm). Please verify the species is correct. Check <a href=http://checker.sccwrp.org/checker/scraper?action=help&layer=lu_invertspeciesdepthrange target=_blank>lu_invertspeciesdepthrange</a> for more information.' %(tam.invertspecies[i],int(tam.startdepth[i]),int(tam.enddepth[i]),tam.mindepth[i],tam.maxdepth[i]),trawlinvertebrateabundance)

    #Jordan - Species - Check list of non-trawl taxa (next tab)
    invalid_species = eng.execute("SELECT species AS invertspecies FROM lu_invertspeciesnotallowed;")
    invs = pd.DataFrame(invalid_species.fetchall()); invs.columns = invalid_species.keys()
    badrows = trawlinvertebrateabundance[trawlinvertebrateabundance.invertspecies.isin(invs.invertspecies.tolist())].tmp_row.tolist(),
    
    'InvertSpecies','Undefined Error','error','Holoplanktonic or infaunal species. See lookup list: <a href=http://checker.sccwrp.org/checker/scraper?action=help&layer=lu_invertspeciesnotallowed target=_blank>lu_invertspeciesnotallowed</a>',trawlinvertebrateabundance)
    
    #Jordan - Anomaly - If Anomaly = Other, a comment is required. 
    #print('If Anomaly = Other, a comment is required.')
    missing_comments = trawlinvertebrateabundance[(trawlinvertebrateabundance.anomaly == 'Other')&((trawlinvertebrateabundance.comments == '')|(trawlinvertebrateabundance.comments.isnull()))]
    print(missing_comments)
    badrows = missing_comments.tmp_row.tolist(),
    
    'Anomaly','Undefined Error','error','A comment is required for all anomalies listed as Other.',trawlinvertebrateabundance)

    
    #Jordan - Anomaly - Check for single records that contain anomalies.
    print('Anomaly - Check for single records that contain anomalies.')
    single_records = trawlinvertebrateabundance[~trawlinvertebrateabundance.duplicated(subset = ['stationid','sampledate','samplingorganization','trawlnumber','invertspecies'],keep=False)]
    print(single_records[single_records.anomaly != 'None'])
    badrows = single_records[single_records.anomaly != 'None'].tmp_row.tolist(),
    
    'Anomaly','Undefined Warning','warning','Anomalies and clean organisms may be lumped together( e.g. 128 urchins, all with parasites isnt likely',trawlinvertebrateabundance) 
    
    ## END ABUNDANCE ONLY CHECKS ##




    return {'errors': errs, 'warnings': warnings}
