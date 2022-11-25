import json, os
from pandas import isnull, DataFrame, to_datetime, read_sql
import math
import numpy as np
from inspect import currentframe
from arcgis.geometry.filters import within, contains
from arcgis.geometry import Point, Polyline, Polygon, Geometry
from arcgis.geometry import lengths, areas_and_lengths, project
import pandas as pd
from flask import current_app
import json


def checkData(tablename, badrows, badcolumn, error_type, error_message = "Error", is_core_error = False, errors_list = [], q = None, **kwargs):
    
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
    print("checkLogic")
    assert \
    set([x.lower() for x in cols]).issubset(set(df1.columns)), \
    "({}) not in columns of {} ({})" \
    .format(
        ','.join([x.lower() for x in cols]), df1_name, ','.join(df1.columns)
    )

    assert \
    set([x.lower() for x in cols]).issubset(set(df2.columns)), \
    "({}) not in columns of {} ({})" \
    .format(
        ','.join([x.lower() for x in cols]), df2_name, ','.join(df2.columns)
    )

    # 'Kristin wrote this code in ancient times.'
    badrows = df1[~df1[cols].isin(df2[cols].to_dict(orient='list')).all(axis=1)].index.tolist()

    print("end checkLogic")

    return {
        "badrows": badrows,
        "badcolumn": ','.join(cols),
        "error_type": "Logic Error",
        "error_message": f"""Each record in {df1_name} must have a matching record in {df2_name}. Records are matched on {','.join(cols)}"""
    }

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
        "error_message": f"""One of the values here is not in the lookup list <a target = "_blank" href=/{current_app.script_root}/scraper?action=help&layer={listname}>{listname}</a>"""
    }

    return args


def check_strata_grab(grab, strata_lookup, field_assignment_table):
    # Get the columns stratum, region from stations_grab_final, merged on stationid.
    # We need these columns to look up for the polygon the stations are supposed to be in
    grab = pd.merge(
        grab, 
        field_assignment_table.filter(items=['stationid','stratum','region']), 
        how='left', 
        on=['stationid']
    )
    # Make the points based on long, lat columns of grab
    grab['SHAPE'] = grab.apply(
        lambda row: Point({                
            "x" :  row['longitude'], 
            "y" :  row['latitude'], 
            "spatialReference" : {'latestWkid': 4326, 'wkid': 4326}
        }),
        axis=1
    )

    # Assert if we are not able to find the lookup strata
    # Strata lookup dictionary has (region, stratum) as keys, so if the region + stratum combination is not in the lookup list, we cannot match
    not_in_field_assignment_table = [(x,y) for x,y in zip(grab['region'], grab['stratum']) if (x,y) not in strata_lookup.keys()]
    assert len(not_in_field_assignment_table) == 0, f"{','.join(not_in_field_assignment_table)} these combos are not in the field_assignment_table" 

    # Now we check if the points are in associated polygon or not. Assign True if they are in
    grab['is_station_in_strata'] = grab.apply(
        lambda row: strata_lookup.get((row['region'], row['stratum'])).contains(row['SHAPE'])
        if strata_lookup.get((row['region'], row['stratum']), None) is not None
        else
        'cannot_find_lookup_strata',
        axis=1
    )

    # Now we get the bad rows
    bad_df = grab.assign(tmp_row=grab.index).query("is_station_in_strata == False")
    return bad_df

def check_strata_trawl(trawl, strata_lookup, field_assignment_table):
    # Get the columns stratum, region from stations_grab_final, merged on stationid.
    # We need these columns to look up for the polygon the stations are supposed to be in
    trawl = pd.merge(
        trawl, 
        field_assignment_table.filter(items=['stationid','stratum','region']), 
        how='left', 
        on=['stationid']
    )

    # Make the points based on long, lat columns of grab
    trawl['SHAPE'] = trawl.apply(
        lambda row: Polyline({                
            "paths" : [
                [
                    [row['startlongitude'], row['startlatitude']], [row['endlongitude'], row['endlatitude']]
                ]
            ],
            "spatialReference" : {"wkid" : 4326}
        }),
        axis=1
    )
    
    # Assert if we are not able to find the lookup strata
    # Strata lookup dictionary has (region, stratum) as keys, so if the region + stratum combination is not in the lookup list, we cannot match
    not_in_field_assignment_table = [(x,y) for x,y in zip(trawl['region'], trawl['stratum']) if (x,y) not in strata_lookup.keys()]
    assert len(not_in_field_assignment_table) == 0, f"{','.join(not_in_field_assignment_table)} these combos are not in the field_assignment_table" 

    # Now we check if the points are in associated polygon or not. Assign True if they are in
    trawl['is_station_in_strata'] = trawl.apply(
        lambda row: strata_lookup.get((row['region'], row['stratum'])).contains(row['SHAPE'])
        if strata_lookup.get((row['region'], row['stratum']), None) is not None
        else
        'cannot_find_lookup_strata',
        axis=1
    )

    # Now we get the bad rows
    bad_df = trawl.assign(tmp_row=trawl.index).query("is_station_in_strata == False")
    return bad_df

def export_sdf_to_json(path, sdf):
    if "paths" in sdf['SHAPE'].iloc[0].keys():
        data = [
            {
                "type":"polyline",
                "paths" : item.get('paths')[0]
            }
            for item in sdf['SHAPE']
        ]
    elif "rings" in sdf['SHAPE'].iloc[0].keys():
        data = [
            {
                "type":"polygon",
                "rings" : item.get('rings')[0]
            }
            for item in sdf['SHAPE']
        ]        
    else:
        data = [
            {
                "type":"point",
                "longitude": item["x"],
                "latitude": item["y"]
            }
            for item in sdf.get("SHAPE").tolist()
        ]
    
    with open(path, "w", encoding="utf-8") as geojson_file:
       json.dump(data, geojson_file)