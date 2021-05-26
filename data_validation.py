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
    :param df (Object): Input dataframe to evaluate the existence assertion
    :param case_num (Int): An integer to denote the case number
    :param flag (Int): A binary integer to distinguish between breadcrumb and trip dataframes 
    :return df (Object): Updated dataframe after deleting the invalid records
    :return case_num (Int): Updated case number
    """

    if flag == 0:
        case_num = case_num + 1
        print('\n----- CASE ' + str(case_num) + ': Every record of Trip table should have a valid',\
                'not NULL vehicle id')
        invalid_record_count = 0
        for item, data in enumerate(df['vehicle_id']):
            if math.isnan(data):   
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
        output = pd.Series(df['trip_id']).is_unique
        for item, data in enumerate(df['trip_id']):
            if math.isnan(data):
                invalid_record_count += 1

        if invalid_record_count > 0 and not output:
            print('--------------Summary ASSERTION VOILATION!! trip ID should be not null and',\
                    'unique value----------')
        elif invalid_record_count == 0 and not output:
            print('--------------SUMMARY ASSERTION VOILATION!! trip ID should be unique value'\
                    '----------')
            df = df.drop_duplicates()
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
        for item, data in enumerate(df['tstamp']):
            if pd.isnull(data):
                invalid_record_count += 1
      
        if invalid_record_count == 0:
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
    :param df (Object): Input breadcrumb dataframe to evaluate the limit assertion
    :param case_num (Int): An integer to denote the case number
    :return df (Object): Updated breadcrumb dataframe after deleting the invalid records
    :return case_num (Int): Updated case number
    """
    case_num = case_num + 1
    print('\n----- CASE ' + str(case_num) + ': Every Breadcrumb record should have',\
            'Direction between 0-359 inclusive --------')
    invalid_record_count = 0
    for item, data in enumerate(df['direction']):
        if pd.isnull(data):
            pass
        elif data >= 0 and data <= 359:
            pass
        else:
            df = df.drop(df.index[item])
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
    for item, data in enumerate(df['speed']):
        if data <= 250 or pd.isnull(data):
            pass
        else:
            df = df.drop(df.index[item])
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
    for item, data in enumerate(df['GPS_SATELLITES']):
        if pd.isnull(data):
            pass
        elif data >= 0 and data <= 12:
            pass
        else:
            df = df.drop(df.index[item])
            invalid_record_count += 1

    if invalid_record_count == 0:
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
    :param df (Object): Input breadcrumb dataframe to evaluate the summary assertion
    :param case_num (Int): An integer to denote the case number
    :return df (Object): Updated breadcrumb dataframe after deleting the invalid records
    :return case_num (Int): Updated case number    
    """
    case_num = case_num + 1
    print('\n----- CASE ' + str(case_num) + ': Every record of Breadcrumb should have unique',\
            'combination of trip id and tstamp')
    summary = list(zip(df['trip_id'], df['tstamp']))
    unique = set()
    duplicate = 0
    nan_values = 0

    for val in summary:
        if pd.isnull(val[0]) or pd.isnull(val[1]):
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
    :param df (Object): Input dataframe to evaluate the referential integrity assertion
    :param case_num (Int): An integer to denote the case number
    :param flag (Int): A binary integer to distinguish between breadcrumb and trip dataframes 
    :return df (Object): Updated dataframe after deleting the invalid records
    :return case_num (Int): Updated case number
    """
    if flag == 0:
        case_num = case_num + 1
        invalid_record_count = 0
        print('\n----- CASE ' + str(case_num) + ': For each vehicle id, there should be a generated',\
                'trip id----------')
        for item, row in df.iterrows():
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
        for item, row in df.iterrows():
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

    return df, case_num

def validate(bc_json_data, se_json_data):

    """
    This is the entry point to this file.
    It performs all the required transformations in the breadcrumb and stop event csv files 
    using pandas dataframes. 
    Also, calls were made to validate these dataframes against various assertions.
    :param bc_json_data (JSON Object): breadcrumb JSON data
    :param se_json_data (JSON Object): Stop Event JSON data
    :return: None
    """
    # show all the rows and columns
    pd.options.display.max_columns = None
    pd.options.display.max_rows = None

    #Read the json file into the dataframe
    df = pd.read_json(bc_json_data)
    df1 = pd.read_json(se_json_data)
    print('\n===================================DATA VALIDATION AND TRANSFORMATION'\
            '====================================\n')

    # TRANSFORMATION 1 : Extract specific selected columns to new DataFrame as a copy
    Breadcrumbdf = df.filter(['OPD_DATE', 'ACT_TIME', 'GPS_LATITUDE', 'GPS_LONGITUDE', \
    'DIRECTION', 'VELOCITY', 'EVENT_NO_TRIP', 'VEHICLE_ID', 'GPS_SATELLITES'], axis=1)
    stopdf = df1.filter(['trip_id', 'vehicle_number', 'route_number', 'direction', 'service_key'], \
    axis=1)
    
    # TRANSFORMATION 2 : Replace all the empty fields by NaN
    Breadcrumbdf = Breadcrumbdf.replace(r'^\s*$', np.nan, regex=True)
    stopdf = stopdf.replace(r'^\s*$', np.nan, regex=True)
    
    # TRANSFORMATION 3 :  Concatenate OPD DATE and ACT TIME --> DATE TIME       
    tstampdf = Breadcrumbdf.filter(['OPD_DATE', 'ACT_TIME'], axis=1)
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
    for index in range(len(tstampdf['OPD_DATE'])):
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
    Breadcrumbdf.insert(0, 'tstamp', timestamps)
    Breadcrumbdf.drop(columns = ['OPD_DATE', 'ACT_TIME'], inplace = True, axis = 1)

    # TRANSFORMATION 5: Rename all the columns of the dataframe to match the schema
    Breadcrumbdf = Breadcrumbdf.rename(columns = {'EVENT_NO_TRIP': 'trip_id', \
    'VELOCITY': 'speed', 'GPS_LONGITUDE': 'longitude', 'GPS_LATITUDE': 'latitude', \
    'DIRECTION': 'direction', 'VEHICLE_ID': 'vehicle_id'})
    stopdf = stopdf.rename(columns = {'vehicle_number': 'vehicle_id', 'route_number': 'route_id'})
    
    # TRANSFORMATION 6 : Convert the Breadcrumb data fields to their respective data types 
    # defined in schema
    Breadcrumbdf['trip_id'] = Breadcrumbdf['trip_id'].astype('Int32')
    Breadcrumbdf['vehicle_id'] = Breadcrumbdf['vehicle_id'].astype('Int32')
    Breadcrumbdf['speed'] = Breadcrumbdf['speed'].astype(float)
    Breadcrumbdf['GPS_SATELLITES'] = Breadcrumbdf['GPS_SATELLITES'].astype(float).astype('Int32')
    Breadcrumbdf['direction'] = Breadcrumbdf['direction'].astype(float).astype('Int32')
    Breadcrumbdf['latitude'] = Breadcrumbdf['latitude'].astype(float)
    Breadcrumbdf['longitude'] = Breadcrumbdf['longitude'].astype(float)
    
    Breadcrumbdf['speed'] = Breadcrumbdf['speed']*2.23694 # change speed from m/s to miles/hr
    
    case_num = 0
    Breadcrumbdf, case_num = existence_assertion(Breadcrumbdf, case_num, flag = 1) # assertion 4
    Breadcrumbdf, case_num = limit_assertion(Breadcrumbdf, case_num) # assertions 5 & 6 & 7
    Breadcrumbdf, case_num = summary_assertions(Breadcrumbdf, case_num) #assertion 8
    Breadcrumbdf, case_num = referential_integrity(Breadcrumbdf, case_num, flag = 1) # assertion 10
    Breadcrumbdf = Breadcrumbdf.drop_duplicates()
    
    # TRANSFORMATION 4 : Convert the stop data fields to their respective data types defined in schema
    stopdf['trip_id'] = stopdf['trip_id'].astype('Int32')
    stopdf['vehicle_id'] = stopdf['vehicle_id'].astype('Int32')
    stopdf['direction'] = stopdf['direction'].astype(str)
    stopdf['service_key'] = stopdf['service_key'].astype(str)
    stopdf['route_id'] = stopdf['route_id'].astype('Int32')

    # TRANSFORMATION 7 : Create a separate view for TRIP DF and add the route_id, service_key 
    # and direction columns with NaN values.
    tripdf = Breadcrumbdf.filter(['trip_id', 'vehicle_id'])
    Breadcrumbdf = Breadcrumbdf.drop("vehicle_id", axis = 1)
    Breadcrumbdf.drop(columns = ['GPS_SATELLITES'], inplace = True, axis = 1)
    tripdf['direction'] = 'Out'
    tripdf['service_key'] = 'Weekday'
    tripdf['route_id'] = np.nan
    #tripdf["direction"] = tripdf["direction"].astype('Int32')
    #tripdf["service_key"] = tripdf["service_key"].astype(str)
    tripdf['route_id'] = tripdf['route_id'].astype('Int32')

    # TRANSFORMATION 4 : Change the value of direction to out and back if its 0 and 1 respectively
    for index in range(len(stopdf['direction'])):
        if stopdf['direction'][index] == '1':
            stopdf['direction'][index] = 'Out'
        elif stopdf['direction'][index] == '0':
            stopdf['direction'][index] = 'Back'
        else:
            stopdf['direction'][index] = 'DELETE'

    # TRANSFORMATION 5: Change W to Weekday, S to Saturday and U to Sunday
    for index in range(len(stopdf['service_key'])):
        if stopdf['service_key'][index] == 'W':
            stopdf['service_key'][index] = 'Weekday'
        elif stopdf['service_key'][index] == 'S':
            stopdf['service_key'][index] = 'Saturday'
        elif stopdf['service_key'][index] == 'U':
            stopdf['service_key'][index] = 'Sunday'
        else:
          stopdf['service_key'][index] = 'DELETE'

    tripdf, case_num = existence_assertion(tripdf, case_num, flag = 0) # assertions 1 & 2 & 3
    tripdf, case_num = referential_integrity(tripdf, case_num, flag = 0) # assertion 9
    
    print('\n=====================For each trip id, we have a single route no, service key',\
            'and direction============')
    groupby_trip = stopdf.groupby('trip_id')
    groups = groupby_trip.groups.keys()
    column_names = ['trip_id', 'vehicle_id', 'route_id', 'direction', 'service_key'] 
    finaldf = pd.DataFrame(columns = column_names)
    for group in groups:
        group_df = groupby_trip.get_group(group)
        groupby_labels = group_df.groupby(['route_id', 'direction', 'service_key'])
        size = max(groupby_labels.size())
        groupby_labels = groupby_labels.filter(lambda x: len(x) == size, dropna = True)
        finaldf = finaldf.append(groupby_labels, ignore_index = True)
    
    finaldf = finaldf.drop_duplicates()
    newdf = tripdf.merge(stopdf, on = ['trip_id', 'vehicle_id'], how = 'left')
    newdf = newdf.drop(newdf.columns[[2, 3, 4]], axis = 1) 
    
    # CONVERT THE DATAFRAMES INTO CSVs
    Breadcrumbdf.to_csv('Breadcrumbdf.csv', index = False, header = False, na_rep = 'None')
    newdf.to_csv('tripdf.csv', index = False, header = False, na_rep = 'None')

