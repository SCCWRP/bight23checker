import json, os
import numpy as np
from inspect import currentframe
import pandas as pd
from flask import current_app

# Method blanks - result has to be less than MDL, or less than 5% of the measured concentration in the sample
def MB_ResultLessThanMDL(dataframe):
    """
    dataframe should have the tmp_row column, and already be filtered according to the "table" (table 5-3, 5-4 etc)
    This function is supposed to return a list of errors
    """

    
    methodblanks = dataframe[dataframe.sampletype == 'Method blank'][['analysisbatchid','matrix','analytename','mdl', 'result']]
    res = dataframe[dataframe.sampletype == 'Result']

    checkdf = res.merge(methodblanks, on = ['analysisbatchid','matrix','analytename'], how = 'inner', suffixes = ('','_mb'))

    checkdf['methodblank_too_high'] = ~checkdf.apply(lambda row: (row.result_mb < row.mdl_mb) | (row.result_mb < (0.05 * row.result)) , axis = 1)

    checkdf = checkdf[checkdf.methodblank_too_high]
    if checkdf.empty:
        return []
    checkdf = checkdf.groupby(['analysisbatchid','analytename','sampleid']).apply(lambda df: df.tmp_row.tolist()).reset_index(name = 'badrows')

    return checkdf.apply(
            lambda row:
            {
                "badrows": row.badrows,
                "badcolumn": "Result",
                "error_type": "Value Error",
                "error_message": f"For the Analyte {row.analytename} in the AnalysisBatch {row.analysisbatchid}, the Method blank result value is either above the MDL, or above 5% of the measured concentration in the sample {row.sampleid}"
            },
            axis = 1
        ).tolist()

