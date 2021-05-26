# -*- coding: utf-8 -*-

import sys
import math
import json 
import csv 
import re
from collections import defaultdict
from datetime import datetime, timedelta

import pandas as pd
import numpy as np


def existence_assertion(df, case_num, flag=None):
    """
    Assertion 1: Every record of Trip table should have a valid not NULL vehicle id 
    Assertion 2 and 3: Every record of Trip table  should have a unique and not NULL trip id
    Assertion 4: Every Breadcrumb record should have a non empty tstamp field
    """

    if(flag == 0):
        case_num = case_num + 1
        print('\n=====================EXISTENCE VALIDATIONS================================')
        print('\n----- CASE ' + str(case_num) + ': Every record of Trip table should have a valid',\
                'not NULL vehicle id')
        invalid_record_count = 0
        for item, data in enumerate(df['vehicle_id']):
            if(math.isnan(data)):   
                invalid_record_count += 1

        if (invalid_record_count == 0):
            print('All the records passed Case {} check!'.format(case_num))
        else:
            print('--------------EXISTENCE ASSERTION VOILATION!! Vehicle ID should not be NULL',\
                    'for Trip records----------')
            print('Count of invalid records: ', invalid_record_count)

        case_num = case_num + 2
        print('\n----- CASE ' + str(case_num-1) + ' & ' + str(case_num) + ': Every record of Trip table',\
	    'should have a unique and not NULL trip id')
        invalid_record_count = 0 
        output = pd.Series(df['trip_id']).is_unique
        for item, data in enumerate(df['trip_id']):
            if(math.isnan(data)):
                invalid_record_count += 1

        if (invalid_record_count > 0 and output == False):
            print('--------------Summary ASSERTION VOILATION!! trip ID should be not null and',\
                    'unique value----------')
        elif (invalid_record_count == 0 and output == False):
            print('--------------SUMMARY ASSERTION VOILATION!! trip ID should be unique value'\
                    '----------')
            df = df.drop_duplicates()
        elif (invalid_record_count > 0 and output == True):
            print('--------------EXISTENCE ASSERTION VOILATION!! trip ID should not be NOT',\
                    'NULL value----------')
            print('Count of invalid records: ', invalid_record_count)
        else:
            print('All the records passed Case {} check!'.format(case_num))
    
    if(flag == 1):
        case_num = case_num + 1
        print('\n----- CASE ' + str(case_num) + ': Every Breadcrumb record should have a non empty',\
                'tstamp field')
        invalid_record_count = 0
        for item, data in enumerate(df['tstamp']):
            if(pd.isnull(data)):
                invalid_record_count += 1
      
        if (invalid_record_count == 0):
            print('All the records passed Case {} check!'.format(case_num))
        else:
            print('--------------EXISTENCE ASSERTION VOILATION!! Every Breadcrumb record',\
                    'should have a non empty tstamp field----------')
            print('Count of invalid records: ', invalid_record_count)

    return df, case_num

def limit_assertion(df, case_num):
    """
    Assertion 5: Direction for each breadcrumb record  should be between 0-359 inclusive
    Assertion 6: The speed field for each breadcrumb record should not exceed 250 miles/hr
    Assertion 7: The number of satellites for breadcrumb record should be between 0 and 12
    """
    print('\n=====================LIMIT VALIDATIONS================================')
    
    case_num = case_num + 1
    print('\n----- CASE ' + str(case_num) + ': Every Breadcrumb record should have',\
            'Direction between 0-359 inclusive --------')
    invalid_record_count = 0
    for item, data in enumerate(df['direction']):
        if(pd.isnull(data)):
            pass
        elif(data >= 0 and data <= 359):
            pass
        else:
            df = df.drop(df.index[item])
            invalid_record_count += 1

    if(invalid_record_count == 0):
        print('All the records passed Case {} check!'.format(case_num))
    else:
        print('--------------LIMIT ASSERTION VOILATION!! Direction should be between 0-359',\
                'inclusive----------')
        print('Count of invalid records: ', invalid_record_count)
    
    case_num = case_num + 1
    print('\n----- CASE ' + str(case_num) + ': The speed field for each breadcrumb record',\
            'should not exceed 250 miles/hr--------')
    invalid_record_count = 0
    for item, data in enumerate(df['speed']):
        if(data <= 250 or pd.isnull(data)):
            pass
        else:
            df = df.drop(df.index[item])
            invalid_record_count += 1
    if(invalid_record_count == 0):
        print('All the records passed Case {} check!'.format(case_num))
    else:
        print('--------------LIMIT ASSERTION VOILATION!! Speed shouldn"t exceed 250',\
                'miles/hr----------')
        print('Count of invalid records: ', invalid_record_count)

    case_num = case_num + 1
    print('\n----- CASE ' + str(case_num) + ': The number of satellites for breadcrumb record',\
            'should be between 0 and 12--------')
    invalid_record_count = 0
    for item, data in enumerate(df['GPS_SATELLITES']):
        if(pd.isnull(data)):
            pass
        elif(data >= 0 and data <= 12):
            pass
        else:
            df = df.drop(df.index[item])
            invalid_record_count += 1

    if(invalid_record_count == 0):
        print('All the records passed Case {} check!'.format(case_num))
    else:
        print('--------------LIMIT ASSERTION VOILATION!! The number of satellites for',\
                'breadcrumb record should be between 0 and 12----------')
        print('Count of invalid records: ', invalid_record_count)

    return df, case_num

def summary_assertions(df, case_num):
    """
    Assertion 8: Across all the Breadcrumb records, combination of trip id and tstamp 
    should be unique
    """
    print('\n=====================SUMMARY VALIDATIONS================================')
    case_num = case_num + 1
    print('\n----- CASE ' + str(case_num) + ': Every record of Breadcrumb should have unique',\
            'combination of trip id and tstamp')
    summary = list(zip(df['trip_id'], df['tstamp']))
    unique = set()
    duplicate = 0
    nan_values = 0

    for val in summary:
        if(pd.isnull(val[0]) or pd.isnull(val[1])):
            nan_values += 1
            continue
        else:
            if val in unique:
                duplicate += 1
            else:
                unique.add(val)
                
    if duplicate == 0:
        print('All the records passed Case {} check!'.format(case_num))
    else:
        print('--------------SUMMARY ASSERTION violation!! combination of trip id and tstamp',\
                'should be unique----------')
        print('Count of invalid records: ', duplicate)

    return df, case_num

def referential_integrity(df, case_num, flag=None):
  """
  Assertion 9: For each vehicle id, there should be a generated  trip id - see the trip table
  Assertion 10: Each breadcrumb record with non zero speed field should have a non-zero
  direction field and vice-versa
  """
  if flag == 0:
    #do assertion 7
    case_num = case_num + 1
    invalid_record_count = 0
    print('\n=====================REFRENTIAL INTEGRITY VALIDATIONS=================='\
            '==============')
    print('\n----- CASE ' + str(case_num) + ': For each vehicle id, there should be a generated',\
            'trip id----------')
    for item, row in df.iterrows():
        trip_id = row['trip_id']
        vehicle_id = row['vehicle_id']
        if (pd.notnull(vehicle_id) and pd.notnull(trip_id) ):
            pass
        else:
            invalid_record_count += 1
        
    if invalid_record_count == 0:
        print('All the records passed Case {} check!'.format(case_num))
    else:
        print('--------------REFRENTIAL INTEGRITY ASSERTION violation!!For each vehicle id,',\
                'there should be a generated trip_id-----------')
        print('Count of invalid records: ', invalid_record_count)
  
  if flag == 1:
    case_num = case_num + 1
    print('\n----- CASE ' + str(case_num) + ': Every Breadcrumb record with nofield should have',\
            'an zero speed non-zero direction field and vice-versa --------')
    invalid_record_count1 = 0
    invalid_record_count2 = 0
    for item, row in df.iterrows():
        speed = row['speed']
        direction = row['direction']
        if(pd.notnull(speed)):
            if(pd.notnull(direction)):
                pass
            else:
                invalid_record_count1 += 1
        else:
            if(pd.notnull(direction)):
                invalid_record_count2 += 1
    
    if(invalid_record_count1 == 0 and invalid_record_count2 == 0):
        print('All the records passed Case {} check!'.format(case_num)) 
    if(invalid_record_count1 > 0):
        print('--------------REFRENTIAL INTEGRITY ASSERTION violation!! Breadcrumb records with',\
                'empty Direction when the speed is non-zero ---------')
        print("Count of invalid records: ", invalid_record_count1) 
    if(invalid_record_count2 > 0):
        print('--------------REFRENTIAL INTEGRITY ASSERTION violation!! Breadcrumb records with',\
                'empty speed when the direction is non-zero ---------')
        print('Count of invalid records: ', invalid_record_count2) 

  return df, case_num

def validate(bc_json_data, se_json_data):
    # show all the rows and columns
    pd.options.display.max_columns = None
    pd.options.display.max_rows = None

    #Read the json file into the dataframe
    df = pd.read_json(bc_json_data)
    df1 = pd.read_json(se_json_data)
    print('\n===================================DATA VALIDATION AND TRANSFORMATION'\
            '====================================\n')

    # TRANSFORMATION 1 : Extract specific selected columns to new DataFrame as a copy
    breadcrumb_df = df.filter(['OPD_DATE', 'ACT_TIME', 'GPS_LATITUDE', 'GPS_LONGITUDE', \
    'DIRECTION', 'VELOCITY', 'EVENT_NO_TRIP', 'VEHICLE_ID', 'GPS_SATELLITES'], axis=1)
    stop_df = df1.filter(['trip_id', 'vehicle_number', 'route_number', 'direction', 'service_key'], \
    axis=1)
    
    # TRANSFORMATION 2 : Replace all the empty fields by NaN
    breadcrumb_df = breadcrumb_df.replace(r'^\s*$', np.nan, regex=True)
    stop_df = stop_df.replace(r'^\s*$', np.nan, regex=True)
    
    # TRANSFORMATION 3 :  Concatenate OPD DATE and ACT TIME --> DATE TIME       
    tstamp_df = breadcrumb_df.filter(['OPD_DATE', 'ACT_TIME'], axis=1)
    timestamps = []
    dic = {
        'JAN': '01',
        'FEB': '02',
        'MAR': '03', 
        'APR': '04',
        'MAY': '05',
        'JUN': '06',
        'JUL': '07',
        'AUG': '08',
        'SEP': '09',
        'OCT': '10',
        'NOV': '11',
        'DEC': '12',
        }
    for index in range(len(tstamp_df['OPD_DATE'])):
        date = df['OPD_DATE'][index][0:2]
        month = dic[df['OPD_DATE'][index][3:6]]
        year = df['OPD_DATE'][index][7:10]
        time = df['ACT_TIME'][index]
        str_time = str(timedelta(seconds = int(time)))
        if 'day' in str_time:
            add_days = ''
            for i in str_time:
                if i == ' ':
                    break
                add_days += i
            date = int(date) + int(add_days)
        str_time = re.sub('[^0-9:]', '', str_time)
        if len(str(date)) == 1:
            date = str(0) + str(date)

        dt = str(date) + month + year + str_time
        tstamp=datetime.strptime(dt, '%d%m%y%H:%M:%S')
        timestamps.append(tstamp)

    # TRANSFORNATION 4 : Drop the "OPD_DATE, ACT_TIME" and insert "tstamp" into Breadcrumb data
    breadcrumb_df.insert(0, 'tstamp', timestamps)
    breadcrumb_df.drop(columns = ['OPD_DATE', 'ACT_TIME'], inplace = True, axis = 1)

    # TRANSFORMATION 5: Rename all the columns of the dataframe to match the schema
    breadcrumb_df = breadcrumb_df.rename(columns = {'EVENT_NO_TRIP': 'trip_id', \
    'VELOCITY': 'speed', 'GPS_LONGITUDE': 'longitude', 'GPS_LATITUDE': 'latitude', \
    'DIRECTION': 'direction', 'VEHICLE_ID': 'vehicle_id'})
    stop_df = stop_df.rename(columns = {'vehicle_number': 'vehicle_id', 'route_number': 'route_id'})
    
    # TRANSFORMATION 6 : Convert the Breadcrumb data fields to their respective data types 
    # defined in schema
    breadcrumb_df['trip_id'] = breadcrumb_df['trip_id'].astype('Int32')
    breadcrumb_df['vehicle_id'] = breadcrumb_df['vehicle_id'].astype('Int32')
    breadcrumb_df['speed'] = breadcrumb_df['speed'].astype(float)
    breadcrumb_df['GPS_SATELLITES'] = breadcrumb_df['GPS_SATELLITES'].astype(float).astype('Int32')
    breadcrumb_df['direction'] = breadcrumb_df['direction'].astype(float).astype('Int32')
    breadcrumb_df['latitude'] = breadcrumb_df['latitude'].astype(float)
    breadcrumb_df['longitude'] = breadcrumb_df['longitude'].astype(float)
    
    breadcrumb_df['speed'] = breadcrumb_df['speed']*2.23694 # change speed from m/s to miles/hr
    
    case_num = 0
    breadcrumb_df, case_num = existence_assertion(breadcrumb_df, case_num, flag = 1) # assertion 4
    breadcrumb_df, case_num = limit_assertion(breadcrumb_df, case_num) # assertions 5 & 6 & 7
    breadcrumb_df, case_num = summary_assertions(breadcrumb_df, case_num) #assertion 8
    breadcrumb_df, case_num = referential_integrity(breadcrumb_df, case_num, flag = 1) # assertion 10
    breadcrumb_df = breadcrumb_df.drop_duplicates()
    
    # TRANSFORMATION 4 : Convert the stop data fields to their respective data types defined in schema
    stop_df['trip_id'] = stop_df['trip_id'].astype('Int32')
    stop_df['vehicle_id'] = stop_df['vehicle_id'].astype('Int32')
    stop_df['direction'] = stop_df['direction'].astype(str)
    stop_df['service_key'] = stop_df['service_key'].astype(str)
    stop_df['route_id'] = stop_df['route_id'].astype('Int32')

    # TRANSFORMATION 7 : Create a separate view for TRIP DF and add the route_id, service_key 
    # and direction columns with NaN values.
    trip_df = breadcrumb_df.filter(['trip_id', 'vehicle_id'])
    breadcrumb_df = breadcrumb_df.drop("vehicle_id", axis = 1)
    breadcrumb_df.drop(columns = ['GPS_SATELLITES'], inplace = True, axis = 1)
    trip_df['direction'] = 'Out'
    trip_df['service_key'] = 'Weekday'
    trip_df['route_id'] = np.nan
    #trip_df["direction"] = trip_df["direction"].astype('Int32')
    #trip_df["service_key"] = trip_df["service_key"].astype(str)
    trip_df['route_id'] = trip_df['route_id'].astype('Int32')

    # TRANSFORMATION 4 : Change the value of direction to out and back if its 0 and 1 respectively
    for index in range(len(stop_df['direction'])):
        if stop_df['direction'][index] == '1':
            stop_df['direction'][index] = 'Out'
        elif stop_df['direction'][index] == '0':
            stop_df['direction'][index] = 'Back'
        else:
            stop_df['direction'][index] = 'DELETE'

    # TRANSFORMATION 5: Change W to Weekday, S to Saturday and U to Sunday
    for index in range(len(stop_df['service_key'])):
        if stop_df['service_key'][index] == 'W':
            stop_df['service_key'][index] = 'Weekday'
        elif stop_df['service_key'][index] == 'S':
            stop_df['service_key'][index] = 'Saturday'
        elif stop_df['service_key'][index] == 'U':
            stop_df['service_key'][index] = 'Sunday'
        else:
          stop_df['service_key'][index] = 'DELETE'

    trip_df, case_num = existence_assertion(trip_df, case_num, flag = 0) # assertions 1 & 2 & 3
    trip_df, case_num = referential_integrity(trip_df, case_num, flag = 0) # assertion 9
    
    #print('\n=====================VALIDATIONS================================')
    print('\n=====================For each trip id, we have a single route no, service key',\
            'and direction============')
    groupby_trip = stop_df.groupby('trip_id')
    groups = groupby_trip.groups.keys()
    column_names = ['trip_id', 'vehicle_id', 'route_id', 'direction', 'service_key'] 
    final_df = pd.DataFrame(columns = column_names)
    for group in groups:
        group_df = groupby_trip.get_group(group)
        groupby_labels = group_df.groupby(['route_id', 'direction', 'service_key'])
        size = max(groupby_labels.size())
        groupby_labels = groupby_labels.filter(lambda x: len(x) == size, dropna = True)
        final_df = final_df.append(groupby_labels, ignore_index = True)
    
    final_df = final_df.drop_duplicates()
    new_df = trip_df.merge(stop_df, on = ['trip_id', 'vehicle_id'], how = 'left')
    new_df = new_df.drop(new_df.columns[[2, 3, 4]], axis = 1) 
    
    # CONVERT THE DATAFRAMES INTO CSVs
    breadcrumb_df.to_csv('Breadcrumbdf.csv', index = False, header = False, na_rep = 'None')
    new_df.to_csv('trip_df.csv', index = False, header = False, na_rep = 'None')

if __name__ == '__main__':
    main()
