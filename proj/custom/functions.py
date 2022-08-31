import json, os
from pandas import isnull, DataFrame, to_datetime, read_sql
import math
import numpy as np
from inspect import currentframe

def checkData(dataframe, tablename, badrows, badcolumn, error_type, is_core_error = False, error_message = "Error", errors_list = [], q = None):
    
    # See comments on the get_badrows function
    # doesnt have to be used but it makes it more convenient to plug in a check
    # that function can be used to get the badrows argument that would be used in this function
    
    if len(badrows) > 0:
        if q is not None:
            # This is the case where we run with multiprocessing
            # q would be a mutliprocessing.Queue() 
            q.put({
                "table": tablename,
                "rows":badrows,
                "columns":badcolumn,
                "error_type":error_type,
                "is_core_error" : is_core_error,
                "error_message":error_message
            })

        return {
            "table": tablename,
            "rows":badrows,
            "columns":badcolumn,
            "error_type":error_type,
            "is_core_error" : is_core_error,
            "error_message":error_message
        }
    return {}
        



# checkLogic() returns indices of rows with logic errors
def checkLogic(df1, df2, cols: list, error_type = "Logic Error", df1_name = "", df2_name = ""):
    ''' each record in df1 must have a corresponding record in df2'''
    print(f"cols: {cols}")
    print(f"df1 cols: {df1.columns.tolist()}")
    print(set([x.lower() for x in cols]).issubset(set(df1.columns)))

    print(f"df2 cols: {df2.columns.tolist()}")
    assert \
    set([x.lower() for x in cols]).issubset(set(df1.columns)), \
    "({}) not in columns of {} ({})" \
    .format(
        ','.join([x.lower() for x in cols]), df1_name, ','.join(df1.columns)
    )
    print("passed 1st assertion")
    assert \
    set([x.lower() for x in cols]).issubset(set(df2.columns)), \
    "({}) not in columns of {} ({})" \
    .format(
        ','.join([x.lower() for x in cols]), df2_name, ','.join(df2.columns)
    )
    print("passed 2nd assertion")
    # 'Kristin wrote this code in ancient times.'
    # 'I still don't fully understand what it does.'
    # all() returns whether all elements are true
    print("before badrows")
    badrows = df1[~df1[[x.lower() for x in cols]].isin(df2[[x.lower() for x in cols]].to_dict(orient='list')).all(axis=1)].index.tolist()
    print(f"badrows: {badrows}")
    print("after badrows")
    #consider raising error if cols list is not str (see mp) --- ask robert though bc maybe nah

    return(badrows)

def mismatch(df1, df2, mergecols):
    #dropping duplicates creates issue of marking incorrect rows
    tmp = df1[mergecols] \
        .merge(
            df2[mergecols].drop_duplicates().assign(present='yes'),
            on = mergecols, 
            how = 'left'
        )

    badrows = tmp[isnull(tmp.present)].index.tolist()
    return badrows


def haversine_np(lon1, lat1, lon2, lat2):
    """
    Calculate the great circle distance between two points
    on the earth (specified in decimal degrees)

    All args must be of equal length.
    """
    lon1, lat1, lon2, lat2 = map(np.radians, [lon1, lat1, lon2, lat2])
    dlon = lon2 - lon1
    dlat = lat2 - lat1
    a = np.sin(dlat/2.0)**2 + np.cos(lat1) * np.cos(lat2) * np.sin(dlon/2.0)**2
    c = 2 * np.arcsin(np.sqrt(a))
    m = 6367000 * c
    return m

def check_time(starttime, endtime):
    df_over = to_datetime(starttime)
    df_start = to_datetime(endtime)
    times = (df_over - df_start).astype('timedelta64[m]')
    return abs(times)

def check_distance(df,start_lat,end_lat,start_lon,end_lon):
    distance = []
    ct = math.pi/180.0 #conversion factor
    for index in df.index:
        dis = math.acos(math.sin(start_lat[index] * ct) * math.sin(end_lat[index] * ct) + math.cos(start_lat[index] * ct)*math.cos(end_lat[index] * ct)*math.cos((end_lon[index] * ct)-(start_lon[index] * ct)))*6371000
        distance.append(dis)
    return distance

def multivalue_lookup_check(df, field, listname, listfield, dbconnection, displayfieldname = None, sep=','):
    """
    Checks a column of a dataframe against a column in a lookup list. Specifically if the column may have multiple values.
    The default is that the user enters multiple values separated by a comma, although the function may take other characters as separators
    
    Parameters:
    df               : The user's dataframe
    field            : The field name of the user's submitted dataframe
    listname         : The Lookup list name (for example lu_resqualcode)
    listfield        : The field of the lookup list table that we are checking against
    displayfieldname : What the user will see in the error report - defaults to the field argument 
                       it should still be a column in the dataframe, but with different capitalization

    Returns a dictionary of arguments to pass to the checkData function
    """

    # default the displayfieldname to the "field" argument
    displayfieldname = displayfieldname if displayfieldname else field

    # displayfieldname should still be a column of the dataframe, but just typically camelcased
    assert displayfieldname.lower() in df.columns, f"the displayfieldname {displayfieldname} was not found in the columns of the dataframe, even when it was lowercased"

    assert field in df.columns, f"In {str(currentframe().f_code.co_name)} (value against multiple values check) - {field} not in the columns of the dataframe"
    lookupvals = set(read_sql(f'''SELECT DISTINCT "{listfield}" FROM "{listname}";''', dbconnection)[listfield].tolist())

    if not 'tmp_row' in df.columns:
        df['tmp_row'] = df.index

    # hard to explain what this is doing through a code comment
    badrows = df[df[field].apply(lambda values: not set([val.strip() for val in str(values).split(sep)]).issubset(lookupvals) )].tmp_row.tolist()
    args = {
        "badrows": badrows,
        "badcolumn": displayfieldname,
        "error_type": "Lookup Error",
        "error_message": f"One of the values here is not in the lookup list <a href=/scraper?action=help&layer={listname}>{listname}</a>"
    }

    return args