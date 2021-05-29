import sys
import time
import argparse
import re
import csv

import psycopg2.extras
import pandas as pd
import numpy as np

db_name = 'postgres'
db_user = 'postgres'
db_pwd = 'postgres'
TableName1 = 'BreadCrumb'
TableName2 = 'Trip'
CreateDB = False  # indicates whether the DB table should be (re)-created

def create_table(conn):
    """
    Create the target table 
    :param conn (object): open connection to a Postgres database (assumes that conn is a valid)
    :return: None
    """
    with conn.cursor() as cursor:
        cursor.execute(f"""
            DROP TABLE IF EXISTS {TableName2} CASCADE;
            DROP TABLE IF EXISTS {TableName1};
            DROP TYPE IF EXISTS service_type;
            DROP TYPE IF EXISTS tripdir_type;
            create type service_type as enum ('Weekday', 'Saturday', 'Sunday');
            create type tripdir_type as enum ('Out', 'Back');
            create table {TableName2} (
            trip_id integer,
            vehicle_id integer,
            direction tripdir_type,
            service_key service_type,
            route_id integer,
            PRIMARY KEY (trip_id)
            );
            create table {TableName1} (
            tstamp timestamp,
            latitude float,
            longitude float,
            direction integer,
            speed float,
            trip_id integer,
            FOREIGN KEY (trip_id) REFERENCES Trip(trip_id)
            );
        """)
    print(f'Created {TableName1}')
    print(f'Created {TableName2}')

def db_connect():
    """
    Connect to the Database
    :return connection (Object): connection to the DB server
    """
    connection = psycopg2.connect(
        host = 'localhost',
        database = db_name,
        user = db_user,
        password = db_pwd,
    )
    connection.autocommit = True
    return connection
    
def load(conn, csvfile, table):
    """
    Load the csvfile to the Postgres table
    :param conn (Object): connection object creates a client session with the db server
    :param csvfile (String): Name of the input csvfile
    :param table (String): Name of the postgres table
    :return: None
    """
    with conn.cursor() as cursor:
        start = time.perf_counter()
        cursor.copy_from(csvfile, table, sep = ',', null = 'None')
        elapsed = time.perf_counter() - start
        print(f'Finished Loading. Elapsed Time: {elapsed:0.4} seconds')
        
def postgres():
    """
    This is the entry point to the code.
    :return: None
    """
    conn = db_connect()
    csvfile1 = open('Breadcrumbdf.csv', 'r')
    csvfile2 = open('tripdf.csv', 'r')
    if CreateDB:
        create_table(conn)
    load(conn, csvfile2, TableName2)
    load(conn, csvfile1, TableName1)
    
