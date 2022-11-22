import json, os
import numpy as np
from inspect import currentframe
import pandas as pd
from flask import current_app
import json

# Typically filter condition should be the analytename restricted to certain values
def chk_required_sampletypes(df, sampletypes, analyteclass, additional_grouping_cols = [], row_index_col = 'tmp_row'):

    # Goal: Construct a list of args (dictionary format) to be passed iteratively into the checkData function in chemistry_custom{
    # {
    #     "badrows": [list of indices],
    #     "badcolumn": "SomeBadColumn",
    #     "error_type": "Missing Required Data",
    #     "error_message": "Some error message"
    # }

    assert row_index_col in df.columns, \
        f"{row_index_col} not found in the columns of the dataframe. \
        It is highly recommended you create a tmp_row column in the dataframe by resetting the index, \
        otherwise you run the risk of incorrectly reporting the rows that contain errors, \
        due to the nature of these checks which require merging and groupby operations with multiple dataframes"

    assert type(additional_grouping_cols) == list, "additional_grouping_cols arg is not a list"
    grouping_columns = ['analysisbatchid', 'analyteclass', *additional_grouping_cols]

    # Assertions - for faster debugging if the function does not get used correctly for whatever reason
    assert set(grouping_columns).issubset(set(df.columns)), f"grouping_columns list ({','.join(grouping_columns)}) not all found in columns of the dataframe"
    assert 'sampletype' in df.columns, "dataframe does not have 'sampletype' column"
    assert type(sampletypes) == list, "grouping_columns arg is not a list"
    assert isinstance(df, pd.DataFrame), "df arg is not a pandas DataFrame"
    assert isinstance(analyteclass, str)

    tmpdf = df[df.analyteclass == analyteclass].groupby(grouping_columns).apply(
        lambda subdf: 
        set(sampletypes) - set(subdf.sampletype.unique())
    ) 
    assert not tmpdf.empty, \
        f"for some reason, after grouping by {','.join(grouping_columns)}, the dataframe came up empty (in required sampletypes function)"
    
    tmpdf = tmpdf.reset_index(name = 'missing_sampletypes')

    tmpdf = tmpdf[tmpdf.missing_sampletypes != set()]

    if tmpdf.empty:
        # No errors found
        args = []
    else:
        checkdf = df.merge(tmpdf, on = grouping_columns, how = 'inner')
        checkdf = checkdf[[*grouping_columns, 'missing_sampletypes', row_index_col]]
        checkdf.missing_sampletypes = checkdf.missing_sampletypes.apply(lambda x: ','.join(x))
        checkdf = checkdf.groupby([*grouping_columns, 'missing_sampletypes']).agg({'tmp_row':list}).reset_index()

        args = checkdf.apply(
            lambda row: 
            {
                "badrows": row.tmp_row,
                "badcolumn": "SampleType",
                "error_type": "Missing Required Data",
                "error_message": f"For the AnalysisBatch {row.analysisbatchid} and AnalyteClass {row.analyteclass}, you are missing the required sampletypes: {row.missing_sampletypes}"
            },
            axis = 1
        ).tolist()

    return args


# Typically filter condition should be the analytename restricted to certain values
def check_required_crm(df, analyteclasses, row_index_col = 'tmp_row'):

    # Goal: Construct a list of args (dictionary format) to be passed iteratively into the checkData function in chemistry_custom{
    # {
    #     "badrows": [list of indices],
    #     "badcolumn": "SomeBadColumn",
    #     "error_type": "Missing Required Data",
    #     "error_message": "Some error message"
    # }
    # Need to check analysis batches and analyteclasses that are missing CRM's. 
    # Only Analyteclasses which require CRM's should be in that analyteclasses list argument

    assert isinstance(analyteclasses, list), "analyteclasses arg is not a list"

    assert row_index_col in df.columns, \
        f"{row_index_col} not found in the columns of the dataframe. \
        It is highly recommended you create a tmp_row column in the dataframe by resetting the index, \
        otherwise you run the risk of incorrectly reporting the rows that contain errors, \
        due to the nature of these checks which require merging and groupby operations with multiple dataframes"

    grouping_columns = ['analysisbatchid', 'analyteclass']

    tmpdf = df[df.analyteclass.isin(analyteclasses)].groupby(grouping_columns).apply(
        lambda subdf: 
        not any(subdf.sampletype.apply(lambda x: 'reference' in str(x).lower()))
    )
    assert not tmpdf.empty, \
        f"for some reason, after grouping by {','.join(grouping_columns)}, the dataframe came up empty (in required sampletypes function)"
    
    tmpdf = tmpdf.reset_index(name = 'missing_crm')

    tmpdf = tmpdf[tmpdf.missing_crm]

    if tmpdf.empty:
        # No errors found
        args = []
    else:
        # checkdf - just a temp variable only intended to be used in this little code block
        checkdf = df.merge(tmpdf, on = grouping_columns, how = 'inner')
        checkdf = checkdf[[*grouping_columns, 'missing_crm', row_index_col]]
        checkdf.missing_crm = checkdf.missing_crm.apply(lambda x: str(x))
        checkdf = checkdf.groupby([*grouping_columns, 'missing_crm']).agg({'tmp_row':list}).reset_index()

        args = checkdf.apply(
            lambda row: 
            {
                "badrows": row.tmp_row,
                "badcolumn": "SampleType",
                "error_type": "Missing Required Data",
                "error_message": f"For the AnalysisBatch {row.analysisbatchid} and AnalyteClass {row.analyteclass}, you are missing a Certified Reference Material"
            },
            axis = 1
        ).tolist()
        
        del checkdf

    return args

def pyrethroid_analyte_logic_check(df, analytes, row_index_col = 'tmp_row'):
    assert 'analysisbatchid' in df.columns, 'analysisbatchid not found in columns of the dataframe'
    assert 'analytename' in df.columns, 'analytename not found in columns of the dataframe'
    assert 'analyteclass' in df.columns, 'analyteclass not found in columns of the dataframe'
    assert row_index_col in df.columns, \
        f"{row_index_col} not found in the columns of the dataframe. \
        It is highly recommended you create a tmp_row column in the dataframe by resetting the index, \
        otherwise you run the risk of incorrectly reporting the rows that contain errors, \
        due to the nature of these checks which require merging and groupby operations with multiple dataframes"

    tmp = df[df.analyteclass == 'Pyrethroid'].groupby('analysisbatchid').apply(
        lambda df:
        set(analytes).issubset(set(df.analytename.unique()))
    )
    if not tmp.empty:
        tmp = tmp.reset_index(name = 'failed_check')
        tmp = df.merge(tmp, on = 'analysisbatchid', how = 'inner')
        badrows = tmp[tmp.analytename.isin(analytes)][row_index_col].tolist()
    else:
        badrows = []
        
    return {
        "badrows": badrows,
        "badcolumn": "AnalysisBatchID, AnalyteName",
        "error_type": "Logic Error",
        "error_message": f"This batch contains both/all of {','.join(analytes)} which is not possible"
    }
