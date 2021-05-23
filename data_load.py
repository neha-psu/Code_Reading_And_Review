import time
import psycopg2.extras
import argparse
import re
import csv
import sys
import pandas as pd
import numpy as np

DBname = "postgres"
DBuser = "postgres"
DBpwd = "postgres"
TableName1 = 'BreadCrumb'
TableName2 = 'Trip'
CreateDB = False  # indicates whether the DB table should be (re)-created
#tmp_df1 = "/home/agrawal/examples/client/cloud/python/trip.csv"
#tmp_df2 = "/home/agrawal/examples/client/cloud/python/breadcrumb.csv"

# create the target table 
# assumes that conn is a valid, open connection to a Postgres database
def createTable(conn):

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

	print(f"Created {TableName1}")
	print(f"Created {TableName2}")

# connect to the database
def dbconnect():

    connection = psycopg2.connect(
        host="localhost",
        database=DBname,
        user=DBuser,
        password=DBpwd,
    )
    connection.autocommit = True
    return connection
    
def load(conn, csvfile, table):

	with conn.cursor() as cursor:
		start = time.perf_counter()
		cursor.copy_from(csvfile, table, sep=",", null='None')
		elapsed = time.perf_counter() - start
		print(f'Finished Loading. Elapsed Time: {elapsed:0.4} seconds')
        
def postgres():
	conn = dbconnect()
	csvfile1 = open('Breadcrumbdf.csv', 'r')
	csvfile2 = open('tripdf.csv', 'r')
	if CreateDB:
		createTable(conn)
	load(conn, csvfile2, TableName2)
	load(conn, csvfile1, TableName1)
    
