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


def existence_assertion(data_frame, case_num, flag=None):
    """
    Assertion 1: Every record of Trip table should have a valid not NULL vehicle id 
    Assertion 2 and 3: Every record of Trip table  should have a unique and not NULL trip id
    Assertion 4: Every Breadcrumb record should have a non empty tstamp field
    :param data_frame (Object): Input dataframe to evaluate the existence assertion
    :param case_num (Int): An integer to denote the case number
    :param flag (Int): A binary integer to distinguish between breadcrumb and trip dataframes 
    :return data_frame (Object): Updated dataframe after deleting the invalid records
    :return case_num (Int): Updated case number
    """

    if flag == 0:
        case_num = case_num + 1
        print('\n----- CASE ' + str(case_num) + ': Every record of Trip table should have a valid',\
                'not NULL vehicle id')
        invalid_record_count = 0
        for item, vehicle_data in enumerate(data_frame['vehicle_id']):
            if math.isnan(vehicle_data):   
                invalid_record_count += 1

        if invalid_record_count == 0:
            print('All the records passed Case {} check!'.format(case_num))
        else:
            print('--------------EXISTENCE ASSERTION VOILATION!! Vehicle ID should not be NULL',\
                    'for Trip records----------')
            print('Count of invalid records: ', invalid_record_count)

        case_num = case_num + 2
        print('\n----- CASE ' + str(case_num-1) + ' & ' + str(case_num) + ': Every record of Trip table',\
	            'should have a unique and not NULL trip id')
        invalid_record_count = 0 
        output = pd.Series(data_frame['trip_id']).is_unique
        for item, trip_data in enumerate(data_frame['trip_id']):
            if math.isnan(trip_data):
                invalid_record_count += 1

        if invalid_record_count > 0 and not output:
            print('--------------Summary ASSERTION VOILATION!! trip ID should be not null and',\
                    'unique value----------')
        elif invalid_record_count == 0 and not output:
            print('--------------SUMMARY ASSERTION VOILATION!! trip ID should be unique value'\
                    '----------')
            data_frame = data_frame.drop_duplicates()
        elif invalid_record_count > 0 and output:
            print('--------------EXISTENCE ASSERTION VOILATION!! trip ID should not be NOT',\
                    'NULL value----------')
            print('Count of invalid records: ', invalid_record_count)
        else:
            print('All the records passed Case {} check!'.format(case_num))
    
    if flag == 1 :
        case_num = case_num + 1
        print('\n----- CASE ' + str(case_num) + ': Every Breadcrumb record should have a non empty',\
                'tstamp field')
        invalid_record_count = 0
        for item, tstamp_data in enumerate(data_frame['tstamp']):
            if pd.isnull(tstamp_data):
                invalid_record_count += 1
      
        if invalid_record_count == 0:
            print('All the records passed Case {} check!'.format(case_num))
        else:
            print('--------------EXISTENCE ASSERTION VOILATION!! Every Breadcrumb record',\
                    'should have a non empty tstamp field----------')
            print('Count of invalid records: ', invalid_record_count)

    return data_frame, case_num

def limit_assertion(data_frame, case_num):
    """
    Assertion 5: Direction for each breadcrumb record  should be between 0-359 inclusive
    Assertion 6: The speed field for each breadcrumb record should not exceed 250 miles/hr
    Assertion 7: The number of satellites for breadcrumb record should be between 0 and 12
    :param data_frame (Object): Input breadcrumb dataframe to evaluate the limit assertion
    :param case_num (Int): An integer to denote the case number
    :return data_frame (Object): Updated breadcrumb dataframe after deleting the invalid records
    :return case_num (Int): Updated case number
    """
    case_num = case_num + 1
    print('\n----- CASE ' + str(case_num) + ': Every Breadcrumb record should have',\
            'Direction between 0-359 inclusive --------')
    invalid_record_count = 0
    for item, direction in enumerate(data_frame['direction']):
        if pd.isnull(direction):
            pass
        elif direction >= 0 and direction <= 359:
            pass
        else:
            data_frame = data_frame.drop(data_frame.index[item])
            invalid_record_count += 1

    if invalid_record_count == 0:
        print('All the records passed Case {} check!'.format(case_num))
    else:
        print('--------------LIMIT ASSERTION VOILATION!! Direction should be between 0-359',\
                'inclusive----------')
        print('Count of invalid records: ', invalid_record_count)
    
    case_num = case_num + 1
    print('\n----- CASE ' + str(case_num) + ': The speed field for each breadcrumb record',\
            'should not exceed 250 miles/hr--------')
    invalid_record_count = 0
    for item, speed in enumerate(data_frame['speed']):
        if speed <= 250 or pd.isnull(speed):
            pass
        else:
            data_frame = data_frame.drop(data_frame.index[item])
            invalid_record_count += 1
    if invalid_record_count == 0:
        print('All the records passed Case {} check!'.format(case_num))
    else:
        print('--------------LIMIT ASSERTION VOILATION!! Speed shouldn"t exceed 250',\
                'miles/hr----------')
        print('Count of invalid records: ', invalid_record_count)

    case_num = case_num + 1
    print('\n----- CASE ' + str(case_num) + ': The number of satellites for breadcrumb record',\
            'should be between 0 and 12--------')
    invalid_record_count = 0
    for item, satellites in enumerate(data_frame['GPS_SATELLITES']):
        if pd.isnull(satellites):
            pass
        elif satellites >= 0 and satellites <= 12:
            pass
        else:
            data_frame = data_frame.drop(data_frame.index[item])
            invalid_record_count += 1

    if invalid_record_count == 0:
        print('All the records passed Case {} check!'.format(case_num))
    else:
        print('--------------LIMIT ASSERTION VOILATION!! The number of satellites for',\
                'breadcrumb record should be between 0 and 12----------')
        print('Count of invalid records: ', invalid_record_count)

    return data_frame, case_num

def summary_assertions(data_frame, case_num):
    """
    Assertion 8: Across all the Breadcrumb records, combination of trip id and tstamp 
                    should be unique
    :param data_frame (Object): Input breadcrumb dataframe to evaluate the summary assertion
    :param case_num (Int): An integer to denote the case number
    :return data_frame (Object): Updated breadcrumb dataframe after deleting the invalid records
    :return case_num (Int): Updated case number    
    """
    case_num = case_num + 1
    print('\n----- CASE ' + str(case_num) + ': Every record of Breadcrumb should have unique',\
            'combination of trip id and tstamp')
    summary_assertion = list(zip(data_frame['trip_id'], data_frame['tstamp']))
    unique = set()
    duplicate = 0
    nan_values = 0

    for summary_data in summary_assertion:
        if pd.isnull(summary_data[0]) or pd.isnull(summary_data[1]):
            nan_values += 1
            continue
        else:
            if summary_data in unique:
                duplicate += 1
            else:
                unique.add(summary_data)
                
    if duplicate == 0:
        print('All the records passed Case {} check!'.format(case_num))
    else:
        print('--------------SUMMARY ASSERTION violation!! combination of trip id and tstamp',\
                'should be unique----------')
        print('Count of invalid records: ', duplicate)

    return data_frame, case_num

def referential_integrity(data_frame, case_num, flag=None):
    """
    Assertion 9: For each vehicle id, there should be a generated  trip id - see the trip table
    Assertion 10: Each breadcrumb record with non zero speed field should have a non-zero
                    direction field and vice-versa
    :param data_frame (Object): Input dataframe to evaluate the referential integrity assertion
    :param case_num (Int): An integer to denote the case number
    :param flag (Int): A binary integer to distinguish between breadcrumb and trip dataframes 
    :return data_frame (Object): Updated dataframe after deleting the invalid records
    :return case_num (Int): Updated case number
    """
    if flag == 0:
        case_num = case_num + 1
        invalid_record_count = 0
        print('\n----- CASE ' + str(case_num) + ': For each vehicle id, there should be a generated',\
                'trip id----------')
        for item, row in data_frame.iterrows():
            trip_id = row['trip_id']
            vehicle_id = row['vehicle_id']
            if pd.notnull(vehicle_id) and pd.notnull(trip_id):
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
        for item, row in data_frame.iterrows():
            speed = row['speed']
            direction = row['direction']
            if pd.notnull(speed):
                if pd.notnull(direction):
                    pass
                else:
                    invalid_record_count1 += 1
            else:
                if pd.notnull(direction):
                    invalid_record_count2 += 1

        if invalid_record_count1 == 0 and invalid_record_count2 == 0:
            print('All the records passed Case {} check!'.format(case_num)) 
        if invalid_record_count1 > 0:
            print('--------------REFRENTIAL INTEGRITY ASSERTION violation!! Breadcrumb records with',\
                    'empty Direction when the speed is non-zero ---------')
            print("Count of invalid records: ", invalid_record_count1) 
        if invalid_record_count2 > 0:
            print('--------------REFRENTIAL INTEGRITY ASSERTION violation!! Breadcrumb records with',\
                    'empty speed when the direction is non-zero ---------')
            print('Count of invalid records: ', invalid_record_count2) 

    return data_frame, case_num

def validate(breadcrumb_json_data, stop_event_json_data):
    """
    This is the entry point to this file.
    It performs all the required transformations in the breadcrumb and stop event csv files 
    using pandas dataframes. 
    Also, calls were made to validate these dataframes against various assertions.
    :param breadcrumb_json_data (JSON Object): breadcrumb JSON data
    :param stop_event_json_data (JSON Object): Stop Event JSON data
    :return: None
    """
    # show all the rows and columns
    pd.options.display.max_columns = None
    pd.options.display.max_rows = None

    # Read the json file into the dataframe
    breadcrumb_data_frame = pd.read_json(breadcrumb_json_data)
    stop_event_data_frame = pd.read_json(stop_event_json_data)
    print('\n===================================DATA VALIDATION AND TRANSFORMATION'\
            '====================================\n')

    # TRANSFORMATION 1 : Extract specific selected columns to new DataFrame as a copy
    breadcrumb_dataframe = breadcrumb_data_frame.filter(['OPD_DATE', 'ACT_TIME', 'GPS_LATITUDE', 'GPS_LONGITUDE', \
    'DIRECTION', 'VELOCITY', 'EVENT_NO_TRIP', 'VEHICLE_ID', 'GPS_SATELLITES'], axis=1)
    stop_dataframe = stop_event_data_frame.filter(['trip_id', 'vehicle_number', 'route_number', 'direction', 'service_key'], \
    axis=1)
    
    # TRANSFORMATION 2 : Replace all the empty fields by NaN
    breadcrumb_dataframe = breadcrumb_dataframe.replace(r'^\s*$', np.nan, regex=True)
    stop_dataframe = stop_dataframe.replace(r'^\s*$', np.nan, regex=True)
    
    # TRANSFORMATION 3 :  Concatenate OPD DATE and ACT TIME --> DATE TIME       
    tstamp_dataframe = breadcrumb_dataframe.filter(['OPD_DATE', 'ACT_TIME'], axis=1)
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
    for index in range(len(tstamp_dataframe['OPD_DATE'])):
        date = breadcrumb_data_frame['OPD_DATE'][index][0:2]
        month = dic[breadcrumb_data_frame['OPD_DATE'][index][3:6]]
        year = breadcrumb_data_frame['OPD_DATE'][index][7:10]
        time = breadcrumb_data_frame['ACT_TIME'][index]
        str_time = str(timedelta(seconds = int(time)))
        if 'day' in str_time:
            add_days = ''
            for day in str_time:
                if day == ' ':
                    break
                add_days += day
            date = int(date) + int(add_days)
        str_time = re.sub('[^0-9:]', '', str_time)
        if len(str(date)) == 1:
            date = str(0) + str(date)

        date_time = str(date) + month + year + str_time
        tstamp=datetime.strptime(date_time, '%d%m%y%H:%M:%S')
        timestamps.append(tstamp)

    # TRANSFORNATION 4 : Drop the "OPD_DATE, ACT_TIME" and insert "tstamp" into Breadcrumb data
    breadcrumb_dataframe.insert(0, 'tstamp', timestamps)
    breadcrumb_dataframe.drop(columns = ['OPD_DATE', 'ACT_TIME'], inplace = True, axis = 1)

    # TRANSFORMATION 5: Rename all the columns of the dataframe to match the schema
    breadcrumb_dataframe = breadcrumb_dataframe.rename(columns = {'EVENT_NO_TRIP': 'trip_id', \
    'VELOCITY': 'speed', 'GPS_LONGITUDE': 'longitude', 'GPS_LATITUDE': 'latitude', \
    'DIRECTION': 'direction', 'VEHICLE_ID': 'vehicle_id'})
    stop_dataframe = stop_dataframe.rename(columns = {'vehicle_number': 'vehicle_id', 'route_number': 'route_id'})
    
    # TRANSFORMATION 6 : Convert the Breadcrumb data fields to their respective data types 
    # defined in schema
    breadcrumb_dataframe['trip_id'] = breadcrumb_dataframe['trip_id'].astype('Int32')
    breadcrumb_dataframe['vehicle_id'] = breadcrumb_dataframe['vehicle_id'].astype('Int32')
    breadcrumb_dataframe['speed'] = breadcrumb_dataframe['speed'].astype(float)
    breadcrumb_dataframe['GPS_SATELLITES'] = breadcrumb_dataframe['GPS_SATELLITES'].astype(float).astype('Int32')
    breadcrumb_dataframe['direction'] = breadcrumb_dataframe['direction'].astype(float).astype('Int32')
    breadcrumb_dataframe['latitude'] = breadcrumb_dataframe['latitude'].astype(float)
    breadcrumb_dataframe['longitude'] = breadcrumb_dataframe['longitude'].astype(float)
    
    breadcrumb_dataframe['speed'] = breadcrumb_dataframe['speed']*2.23694 # change speed from m/s to miles/hr
    
    case_num = 0
    breadcrumb_dataframe, case_num = existence_assertion(breadcrumb_dataframe, case_num, flag = 1) # assertion 4
    breadcrumb_dataframe, case_num = limit_assertion(breadcrumb_dataframe, case_num) # assertions 5 & 6 & 7
    breadcrumb_dataframe, case_num = summary_assertions(breadcrumb_dataframe, case_num) #assertion 8
    breadcrumb_dataframe, case_num = referential_integrity(breadcrumb_dataframe, case_num, flag = 1) # assertion 10
    breadcrumb_dataframe = breadcrumb_dataframe.drop_duplicates()
    
    # TRANSFORMATION 4 : Convert the stop data fields to their respective data types defined in schema
    stop_dataframe['trip_id'] = stop_dataframe['trip_id'].astype('Int32')
    stop_dataframe['vehicle_id'] = stop_dataframe['vehicle_id'].astype('Int32')
    stop_dataframe['direction'] = stop_dataframe['direction'].astype(str)
    stop_dataframe['service_key'] = stop_dataframe['service_key'].astype(str)
    stop_dataframe['route_id'] = stop_dataframe['route_id'].astype('Int32')

    # TRANSFORMATION 7 : Create a separate view for TRIP Dataframe and add the route_id, service_key 
    # and direction columns with NaN values.
    trip_dataframe = breadcrumb_dataframe.filter(['trip_id', 'vehicle_id'])
    breadcrumb_dataframe = breadcrumb_dataframe.drop("vehicle_id", axis = 1)
    breadcrumb_dataframe.drop(columns = ['GPS_SATELLITES'], inplace = True, axis = 1)
    trip_dataframe['direction'] = 'Out'
    trip_dataframe['service_key'] = 'Weekday'
    trip_dataframe['route_id'] = np.nan
    # trip_dataframe["direction"] = trip_dataframe["direction"].astype('Int32')
    # trip_dataframe["service_key"] = trip_dataframe["service_key"].astype(str)
    trip_dataframe['route_id'] = trip_dataframe['route_id'].astype('Int32')

    # TRANSFORMATION 4 : Change the value of direction to out and back if its 0 and 1 respectively
    for index in range(len(stop_dataframe['direction'])):
        if stop_dataframe['direction'][index] == '1':
            stop_dataframe['direction'][index] = 'Out'
        elif stop_dataframe['direction'][index] == '0':
            stop_dataframe['direction'][index] = 'Back'
        else:
            stop_dataframe['direction'][index] = 'DELETE'

    # TRANSFORMATION 5: Change W to Weekday, S to Saturday and U to Sunday
    for index in range(len(stop_dataframe['service_key'])):
        if stop_dataframe['service_key'][index] == 'W':
            stop_dataframe['service_key'][index] = 'Weekday'
        elif stop_dataframe['service_key'][index] == 'S':
            stop_dataframe['service_key'][index] = 'Saturday'
        elif stop_dataframe['service_key'][index] == 'U':
            stop_dataframe['service_key'][index] = 'Sunday'
        else:
          stop_dataframe['service_key'][index] = 'DELETE'

    trip_dataframe, case_num = existence_assertion(trip_dataframe, case_num, flag = 0) # assertions 1 & 2 & 3
    trip_dataframe, case_num = referential_integrity(trip_dataframe, case_num, flag = 0) # assertion 9
    
    print('\n=====================For each trip id, we have a single route no, service key',\
            'and direction============')
    groupby_trip = stop_dataframe.groupby('trip_id')
    groups = groupby_trip.groups.keys()
    column_names = ['trip_id', 'vehicle_id', 'route_id', 'direction', 'service_key'] 
    final_dataframe = pd.DataFrame(columns = column_names)
    for group in groups:
        group_dataframe = groupby_trip.get_group(group)
        groupby_labels = group_dataframe.groupby(['route_id', 'direction', 'service_key'])
        size = max(groupby_labels.size())
        groupby_labels = groupby_labels.filter(lambda label: len(label) == size, dropna = True)
        final_dataframe = final_dataframe.append(groupby_labels, ignore_index = True)
    
    final_dataframe = final_dataframe.drop_duplicates()
    new_dataframe = trip_dataframe.merge(stop_dataframe, on = ['trip_id', 'vehicle_id'], how = 'left')
    new_dataframe = new_dataframe.drop(new_dataframe.columns[[2, 3, 4]], axis = 1) 
    
    # CONVERT THE DATAFRAMES INTO CSVs
    breadcrumb_dataframe.to_csv('Breadcrumbdf.csv', index = False, header = False, na_rep = 'None')
    new_dataframe.to_csv('trip_df.csv', index = False, header = False, na_rep = 'None')

