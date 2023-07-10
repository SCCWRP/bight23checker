# Dont touch this file! This is intended to be a template for implementing new custom checks

from inspect import currentframe
from flask import current_app, g
from .functions import checkData, mismatch
import pandas as pd
import numpy as np

def fish(all_dfs):
    print("Start Fish Custom Checks")
    
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

    trawlfishabundance = all_dfs['tbl_trawlfishabundance']
    trawlfishbiomass = all_dfs['tbl_trawlfishbiomass']

    trawlfishabundance['tmp_row'] = trawlfishabundance.index
    trawlfishbiomass['tmp_row'] = trawlfishbiomass.index


    trawlfishabundance_args = {
        "dataframe": trawlfishabundance,
        "tablename": 'tbl_trawlfishabundance',
        "badrows": [],
        "badcolumn": "",
        "error_type": "",
        "is_core_error": False,
        "error_message": ""
    }

    trawlfishbiomass_args = {
        "dataframe": trawlfishbiomass,
        "tablename": 'tbl_trawlfishbiomass',
        "badrows": [],
        "badcolumn": "",
        "error_type": "",
        "is_core_error": False,
        "error_message": ""
    }
    eng = g.eng

    # Logic check - each record in the trawlfish abundance and biomass has to have a corresponding record in the tbl_trawlevent #
    print("Fish Custom Checks")
    print("each record in the trawlfish abundance and biomass has to have a corresponding record in the tbl_trawlevent")
    matchcols = ['stationid','sampledate','samplingorganization','trawlnumber']
    trawlevent = pd.read_sql("SELECT stationid,sampledate,samplingorganization,trawlnumber FROM tbl_trawlevent;", eng)
    
    trawlfishabundance_args.update({
        "badrows": mismatch(trawlfishabundance, trawlevent, matchcols),
        "badcolumn": ",".join(matchcols),
        "error_type": "Logic Error",
        "error_message": f"Each record in trawlfishabundance must have a corresponding record in tbl_trawlevent. Records are matched based on {', '.join(matchcols)}"
    })
    errs = [*errs, checkData(**trawlfishabundance_args)]
    
    trawlfishbiomass_args.update({
        "badrows": mismatch(trawlfishbiomass, trawlevent, matchcols),
        "badcolumn": ",".join(matchcols),
        "error_type": "Logic Error",
        "error_message": f"Each record in trawlfishbiomass must have a corresponding record in tbl_trawlevent. Records are matched based on {', '.join(matchcols)}"
    })
    errs = [*errs, checkData(**trawlfishbiomass_args)]

    
    # Abundance Checks
    # 1. User is required to enter an anomaly, but multple anomalies are allowed to be entered
    print("Fish Custom Checks")
    print("User is required to enter an anomaly, but multple anomalies are allowed to be entered")
    badrows = trawlfishabundance[
        trawlfishabundance.anomaly.apply(
            lambda x: 
            not set([substring.strip() for substring in str(x).split(',')]).issubset(set(pd.read_sql("SELECT DISTINCT anomaly FROM lu_fishanomalies", eng).anomaly.tolist()))
        )
    ].index.tolist()
    trawlfishabundance_args.update({
        "badrows": badrows,
        "badcolumn": "Anomaly",
        "error_type": "Lookup Error",
        "error_message": f"You are required to enter at least one fish anomaly, and they must all be found in the <a href=/{current_app.script_root}/scraper?action=help&layer=lu_fishanomalies target=_blank>lookup list</a>. If entering multiple anomalies, they must be separated by commas."
    })
    errs = [*errs, checkData(**trawlfishabundance_args)]

    # Check species abundance totals and corresponding anomalies - warn if abundance for a fish with an anomaly is greater than one
    print("# Check species abundance totals and corresponding anomalies - warn if abundance for a fish with an anomaly is greater than one")
    badrows = trawlfishabundance[
        (trawlfishabundance.anomaly != 'None') & (trawlfishabundance.abundance > 1)
    ].index.tolist()
    trawlfishabundance_args.update({
        "badrows": badrows,
        "badcolumn": "Abundance",
        "error_type": "Undefined Warning",
        "error_message": "There is an anomaly here, but the abundance is greater than 1. This is uncommon."
    })
    warnings.append(checkData(**trawlfishabundance_args))
    
    # 2. User is required to enter a QA Code, but multple anomalies are allowed to be entered
    print("Fish Custom Checks")
    print("User is required to enter a QA Code, but multple anomalies are allowed to be entered")
    badrows = trawlfishabundance[
        trawlfishabundance.abundancequalifier.apply(
            lambda x: 
            not set([substring.strip() for substring in str(x).split(',')]).issubset(set(pd.read_sql("SELECT DISTINCT qualifier FROM lu_trawlqualifier", eng).qualifier.tolist()))
        )
    ].index.tolist()
    trawlfishabundance_args.update({
        "badrows": badrows,
        "badcolumn": "AbundanceQualifier",
        "error_type": "Lookup Error",
        "error_message": f"You are required to enter at least one qualifier code, and they must all be found in the <a href=/{current_app.script_root}/scraper?action=help&layer=lu_trawlqualifier target=_blank>lookup list</a>. If entering multiple qualifiers, they must be separated by commas."
    })
    errs = [*errs, checkData(**trawlfishabundance_args)]

    print("Fish Custom Checks")
    print("Comment required for anomalies Skeletal, Tumor or Lesion")
    badrows = trawlfishabundance[
        trawlfishabundance[['anomaly','comments']] \
            .replace(np.NaN,'').replace(pd.NA,'').apply(
                lambda x: 
                ( 
                    len(set([s.strip() for s in str(x.anomaly).split(',')]).intersection(set(['Deformity (Skeletal)','Tumor','Lesion']))) > 0
                )
                &
                (
                    str(x.comments) == ''
                ), 
                axis=1
            )
        ] \
        .index.tolist()
    trawlfishabundance_args.update({
        "badrows": badrows,
        "badcolumn": "Comments",
        "error_type": "Undefined Error",
        "error_message": "A comment is required for records that have anomalies 'Deformity (Skeletal)', 'Tumor', or 'Lesion'"
    })
    errs = [*errs, checkData(**trawlfishabundance_args)]


    # 3. Return error if abundance records are not found in field assignment table
    print("Fish Custom Checks")
    print("Return error if abundance records are not found in field assignment table")
    fat = pd.read_sql("""SELECT stationid,assigned_agency AS trawlagency FROM field_assignment_table WHERE "parameter" = 'trawl';""", eng)
    
    unique_fat_records = [] if fat.empty else fat.apply(lambda row: (row.stationid, row.trawlagency), axis = 1).tolist()
    

    
    # compare abundance records to field assignment table records (compare on stationid,samplingorganization).
    badrows = trawlfishabundance[
        trawlfishabundance[['stationid','samplingorganization']].apply(lambda x: (x.stationid,x.samplingorganization) not in unique_fat_records, axis=1)
    ].index.tolist()
    trawlfishabundance_args.update({
        "badrows": badrows,
        "badcolumn": "StationID,SamplingOrganization",
        "error_type": "Undefined Error",
        "error_message": "You have submitted stations that are not bight stations or were not assigned to your organization."
    })
    errs = [*errs, checkData(**trawlfishabundance_args)]


    # 4. Jordan - Range check - Group by fish species and get the size class ranges (both the min & max). Compare to lu_fishspeciesdepthrange table and minimumdepth/maximumdepth fields.
    print("Fish Custom Checks")
    print("Range check - Group by fish species and get the size class ranges (both the min & max). Compare to lu_fishspeciesdepthrange table and minimumdepth/maximumdepth fields.")
    lu_sizeranges = eng.execute("SELECT scientificname as fishspecies,maximumsizeclass FROM lu_fishspeciesdepthrange;")
    size_ranges = pd.DataFrame(lu_sizeranges.fetchall())
    size_ranges.columns = lu_sizeranges.keys()
    # check that submitted sizeclass is within range on lookuplist
    svr = trawlfishabundance[['fishspecies','sizeclass','tmp_row']].reset_index().merge(size_ranges,on='fishspecies').set_index('index')
    badrows = svr[(svr.sizeclass>svr.maximumsizeclass)&(svr.fishspecies.isin(size_ranges[size_ranges.maximumsizeclass != -88].fishspecies.tolist()))].index.tolist()
    trawlfishabundance_args.update({
        "badrows": badrows,
        "badcolumn": "FishSpecies, SizeClass",
        "error_type": "Range Error",
        "error_message": f"The size class for these fish are above the maximum recorded. Please verify the species and size class are correct. Check <a href=/{current_app.script_root}/scraper?action=help&layer=lu_fishspeciesdepthrange target=_blank>lu_fishspeciesdepthrange</a> for more information."
    })
    errs = [*errs, checkData(**trawlfishabundance_args)]


    # 5. Jordan - Range check - Check depth ranges (min & max) for all fish species.
    print("Fish Custom Checks")
    print("Range check - Check depth ranges (min & max) for all fish species.")
    # 1st. Get StartDepth and EndDepth for each unique StationID/SampleDate/SamplingOrganization record from tbl_trawlevent
    print("# 1st. Get StartDepth and EndDepth for each unique StationID/SampleDate/SamplingOrganization record from tbl_trawlevent")
    td = pd.read_sql("SELECT stationid,sampledate,samplingorganization,startdepth,enddepth FROM tbl_trawlevent;",eng)
    print('td')
    print(td)
    
    # 2nd. Get Min and Max Depths for each Species from lu_fishspeciesdepthrange
    print("# 2nd. Get Min and Max Depths for each Species from lu_fishspeciesdepthrange")
    depth_ranges = pd.read_sql("SELECT scientificname as fishspecies,minimumdepth,maximumdepth FROM lu_fishspeciesdepthrange;", eng)
    
    # 3rd. Merge Trawl Depth Records and Submitted Fish Abundance Records on StationID/SampleDate/SamplingOrganization
    print("# 3rd. Merge Trawl Depth Records and Submitted Fish Abundance Records on StationID/SampleDate/SamplingOrganization")
    tam = trawlfishabundance[['stationid','sampledate','samplingorganization','fishspecies','tmp_row']].merge(depth_ranges,on='fishspecies').merge(td,on=['stationid','sampledate','samplingorganization'])
    print("Done merging")
    
    print("tam.apply(lambda x: not ((max(x.startdepth,x.enddepth)<x.minimumdepth)|(min(x.startdepth,x.enddepth)>x.maximumdepth)), axis = 1)")
    print(tam.apply(lambda x: not ((max(x.startdepth,x.enddepth)<x.minimumdepth)|(min(x.startdepth,x.enddepth)>x.maximumdepth)), axis = 1))
    
    if not tam.empty:
        tam['inrange'] = tam.apply(lambda x: not ((max(x.startdepth,x.enddepth)<x.minimumdepth)|(min(x.startdepth,x.enddepth)>x.maximumdepth)), axis = 1)
        print("Done creating inrange column")

        print("tam")
        print(tam)
        for i in tam[tam.inrange == False].index.tolist():
            # The way the check is written, is so that each species gets its own error message
            # So badrows will always be an individual value in this case
            # So it is an integer. Therefore we need to put it in a list so it can work with the checkData function
            badrows = [tam.iloc[i].tmp_row.tolist()]
            trawlfishabundance_args.update({
                "badrows": badrows,
                "badcolumn": "FishSpecies",
                "error_type": "Undefined Warning",
                "error_message": '{} was caught in a depth range ({}m - {}m) that does not include the range it is typically found ({}m - {}m). Please verify the species is correct. Check <a href=/{}/scraper?action=help&layer=lu_fishspeciesdepthrange target=_blank>lu_fishspeciesdepthrange</a> for more information.'.format(tam.fishspecies[i],int(tam.startdepth[i]),int(tam.enddepth[i]),tam.minimumdepth[i],tam.maximumdepth[i],current_app.script_root)
            })
            warnings = [*warnings, checkData(**trawlfishabundance_args)]



    print("Fish Custom Checks")
    print("Cross table checks - abundance vs. biomass  Link both abundance and biomass submissions and run mismatch query to check for orphan records.")
    # 6. Cross table checks - abundance vs. biomass  Link both abundance and biomass submissions and run mismatch query to check for orphan records. 
    #    "Composite weight" should be only mismatch.  Error message - Orphan records for biomass vs abundance.
    # Its a problem if the record is in biomass but not abundance
    matchcols = ['stationid','sampledate','samplingorganization','fishspecies']
    trawlfishbiomass_args.update({
        "badrows": mismatch(trawlfishbiomass, trawlfishabundance, matchcols),
        "badcolumn": ",".join(matchcols),
        "error_type": "Logic Error",
        "error_message": f"Each record in biomass must match a record in abundance - records are matched based on {','.join(matchcols)}"
    })
    errs = [*errs, checkData(**trawlfishbiomass_args)]

    
    # Biomass checks
    print("Fish Custom Checks")
    print("If it was submitted as 0 it should rather be submitted as <0.1kg")
    # If it was submitted as 0 it should rather be submitted as <0.1kg
    trawlfishbiomass_args.update({
        "badrows": trawlfishbiomass[trawlfishbiomass.biomass == 0].tmp_row.tolist(),
        "badcolumn": "Biomass",
        "error_type": "Undefined Error",
        "error_message": 'Weight submitted as 0 should have been <0.1kg'
    })
    errs = [*errs, checkData(**trawlfishbiomass_args)]


    print("Fish Custom Checks")
    print("If biomass was measured with greater resolution than what is required in the IM plan ( only one decimal place is allowed), data should be rounded to the nearest 0.1")
    # If biomass was measured with greater resolution than what is required in the IM plan ( only one decimal place is allowed), data should be rounded to the nearest 0.1
    trawlfishbiomass['biomass'] = [round(trawlfishbiomass['biomass'][x], 1) for x in trawlfishbiomass.index]
    trawlfishbiomass_args.update({
        "badrows": trawlfishbiomass[(trawlfishbiomass['biomass'] < .1) & ~(trawlfishbiomass['biomassqualifier'].isin(['less than']))].index.tolist(),
        "badcolumn": "Biomass,BiomassQualifier",
        "error_type": "Undefined Error",
        "error_message": """Biomass values that were less than 0.1 kg (e.g. 0.004 kg) should have been submitted as <0.1 kg (.1 in biomass column, and 'less than' in the biomass qualifier column"""
    })
    errs = [*errs, checkData(**trawlfishbiomass_args)]
    
    print("Fish Custom Checks")
    print("if using < qualifier, biomass value should be 0.1.")
    # if using < qualifier, biomass value should be 0.1.
    trawlfishbiomass_args.update({
        "badrows": trawlfishbiomass[(trawlfishbiomass['biomassqualifier'].isin(['less than', '<']))&~(trawlfishbiomass['biomass'] == 0.1)].index.tolist(),
        "badcolumn": "Biomass,BiomassQualifier",
        "error_type": "Undefined Error",
        "error_message": 'if using < qualifier, biomass value should be 0.1. Units are always kg.'
    })
    errs = [*errs, checkData(**trawlfishbiomass_args)]


    print("Fish Custom Checks")
    print("compare biomass records to field assignment table records (compare on stationid,samplingorganization).")
    # compare biomass records to field assignment table records (compare on stationid,samplingorganization).
    # same check exists for abundance
    badrows = trawlfishbiomass[
        trawlfishbiomass[['stationid','samplingorganization']].apply(lambda x: (x.stationid,x.samplingorganization) not in unique_fat_records, axis=1)
    ].index.tolist()
    trawlfishbiomass_args.update({
        "badrows": badrows,
        "badcolumn": "StationID,SamplingOrganization",
        "error_type": "Undefined Error",
        "error_message": "You have submitted stations that are not bight stations or were not assigned to your organization."
    })
    errs = [*errs, checkData(**trawlfishbiomass_args)]
     

    print("Fish Custom Checks")
    print("Check biomass ranges (min&max) for each taxon at each station.")
    #Kristin - Check biomass ranges (min&max) for each taxon at each station.  Error - Impossibly large/questionable biomass values subgmitted for low abundances of extremely small taxa
    for spec in trawlfishbiomass.fishspecies.unique():
        badrows = trawlfishbiomass[(trawlfishbiomass.fishspecies == spec)&(trawlfishbiomass.biomass > 2 * sorted(trawlfishbiomass.biomass, reverse = True)[2])].tmp_row.tolist()
        if len(badrows) > 0:
            trawlfishbiomass_args.update({
                "badrows": badrows,
                "badcolumn": "Biomass",
                "error_type": "Undefined Warning",
                "error_message": "Impossibly large/questionable biomass values submitted for low abundances of extremely small taxa"
            })
            warnings = [*warnings, checkData(**trawlfishbiomass_args)]

    # End of fish checks
    print('End of fish checks')

    return {'errors': errs, 'warnings': warnings}
