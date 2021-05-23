import time
import psycopg2.extras
import argparse
import re
import csv
import sys
import pandas as pd
import numpy as np
import csv, json
from geojson import Feature, FeatureCollection, Point


DBname = "postgres"
DBuser = "postgres"
DBpwd = "postgres"

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

def result_to_geojson(features, fname):
    collection = FeatureCollection(features)
    with open(fname, "w") as f:
        f.write('%s' % collection)
        
def query1(conn):
    """ A heatmap of a single trip for any bus route that crosses the Glenn Jackson I-205 bridge. 
        You choose the day, time and route for your selected trip.
    """
    cur = conn.cursor()
    cur.execute("DROP view IF EXISTS i205")
    cur.execute("Create view i205 as Select t.trip_id from breadcrumb b, trip t where b.trip_id=t.trip_id and latitude > 45.586158 and latitude < 45.592404  and longitude>-122.550711 and longitude<-122.541270 and t.trip_id = 170031016  and route_id = 164 and date(tstamp) = '2020-10-09'")
    cur.execute("Select b.speed, b.latitude, b.longitude,b.tstamp::time as time from breadcrumb b,trip t, i205 as i where b.trip_id=t.trip_id and t.trip_id = i.trip_id and t.route_id = 164 and tstamp::time between time '08:00:00' and '10:00:00' and date(tstamp) = '2020-10-09'")
    rows = cur.fetchall()
    print("The row count for query 1: ", cur.rowcount)
    features = []
    for row in rows:
        speed = row[0]
        if speed == "" or speed is None:
            continue
        lat = row[1]
        longit = row[2]
        #time = row[3]
        if lat == "" or lat is None or longit == "" or longit is None:
            continue
        try:
            latitude, longitude = map(float, (lat, longit))
            features.append(
                Feature(
                    geometry = Point((longitude,latitude)),
                    properties = {
                        'speed': (int(speed))
                    }
                )
            )
        except ValueError:
            continue
    fname = "query1.geojson"
    result_to_geojson(features, fname)
    cur.close()  

def query2(conn):
    """ 
    All outbound trips that occurred on route 65 on any Friday 
    (you choose which Friday) between the hours of 4pm and 6pm.
    """
    cur = conn.cursor()
    cur.execute("DROP view IF EXISTS DAYS")
    cur.execute("create view  DAYS as select *,extract(isodow from TSTAMP) from breadcrumb")
    cur.execute("select t.trip_id,b.speed, b.latitude, b.longitude from  trip t, DAYS b where t.trip_id=b.trip_id and t.direction='Out' and t.route_id=65 and date_part=5 and date(tstamp) = '2020-10-09' and tstamp::time between time '16:00:00' and '18:00:00'")
    rows = cur.fetchall()
    print("The row count for query 2: ", cur.rowcount)
    features = []
    for row in rows:
        #trip_id = row[0]
        speed = row[1]
        if speed == "" or speed is None:
            continue
        lat = row[2]
        longit = row[3]
        if lat == "" or lat is None or longit == "" or longit is None:
            continue
        try:
            latitude, longitude = map(float, (lat, longit))
            features.append(
                Feature(
                    geometry = Point((longitude,latitude)),
                    properties = {
                        'speed': (int(speed))
                    }
                )
            )
        except ValueError:
            continue
    fname = "query2.geojson"
    result_to_geojson(features, fname)
    cur.close()  
    
def query3(conn):
    """ All outbound trips for route 65 on any 
    Sunday morning (you choose which Sunday) between 9am and 11am

    """
    cur = conn.cursor()
    cur.execute("select t.trip_id,b.speed, b.latitude, b.longitude from  trip t,breadcrumb b where t.trip_id=b.trip_id and t.direction = 'Out' and service_key='Sunday' and date(tstamp) = '2020-10-11' and route_id=65 and tstamp::time between time '09:00:00' and '11:00:00'") 
    rows = cur.fetchall()
    print("The row count for query 3: ", cur.rowcount)
    features = []
    for row in rows:
       # trip_id = row[0]
        speed = row[1]
        if speed == "" or speed is None:
            continue
        lat = row[2]
        longit = row[3]
        if lat == "" or lat is None or longit == "" or longit is None:
            continue
        try:
            latitude, longitude = map(float, (lat, longit))
            features.append(
                Feature(
                    geometry = Point((longitude,latitude)),
                    properties = {
                        'speed': (int(speed))
                    }
                )
            )
        except ValueError:
            continue
    fname = "query3.geojson"
    result_to_geojson(features, fname)
    cur.close()  

def query4(conn):
    """ 
    The longest (as measured by time) trip in your entire data set.
    """
    cur = conn.cursor()
    cur.execute("DROP view IF EXISTS longest")
    cur.execute("Create view longest as select age(max(b.tstamp), min(b.tstamp)) as time, b.trip_id, DATE(tstamp), t.route_id from breadcrumb b, trip t where b.trip_id=t.trip_id group by b.trip_id, date(tstamp), t.route_id order by time desc limit 1")
    cur.execute("Select speed, latitude, longitude from breadcrumb b, longest l where b.trip_id=l.trip_id")
    
    rows = cur.fetchall()
    print("The row count for query 4: ", cur.rowcount)
    features = []
    for row in rows:
        speed = row[0]
        if speed == "" or speed is None:
            continue
        lat = row[1]
        longit = row[2]
        if lat == "" or lat is None or longit == "" or longit is None:
            continue
        try:
            latitude, longitude = map(float, (lat, longit))
            features.append(
                Feature(
                    geometry = Point((longitude,latitude)),
                    properties = {
                        'speed': (int(speed))
                    }
                )
            )
        except ValueError:
            continue
    fname = "query4.geojson"
    result_to_geojson(features, fname)
    cur.close()  

def query5a(conn):
    """
    Speeds for all return onward trips in the I-5 Bridge between 6 PM and 8 PM on Sunday October 11, 2020
    """

    cur = conn.cursor()
    cur.execute("Select t.trip_id, speed, latitude, longitude, tstamp::time as time from breadcrumb b,trip t where b.trip_id=t.trip_id and latitude> 45.615477 and latitude < 45.620460 and longitude>-122.677744 and longitude<-122.673624 and DATE(tstamp) = '2020-10-11' and tstamp::time between time '18:00:00' and '20:00:00' and t.direction='Back'")
    rows = cur.fetchall()
    print("The row count for query 5: ", cur.rowcount)
    features = []
    for row in rows:
        #trip_id = row[0]
        speed = row[1]
        if speed == "" or speed is None:
            continue
        lat = row[2]
        longit = row[3]
        #time = row[4]
        if lat == "" or lat is None or longit == "" or longit is None:
            continue
        try:
            latitude, longitude = map(float, (lat, longit))
            features.append(
                Feature(
                    geometry = Point((longitude,latitude)),
                    properties = {
                        'speed': (int(speed))
                    }
                )
            )
        except ValueError:
            continue
    fname = "query5a.geojson"
    result_to_geojson(features, fname)
    cur.close()    

def query5b(conn):
    """
    Visualize the CTRAN buses travelling with the speed above 55 miles/hr on route 71 on October 12, 2020.
    """

    cur = conn.cursor()
    cur.execute("Select t.trip_id, b.speed, b.latitude, b.longitude, date(tstamp) as tstamp from breadcrumb b, trip t where b.trip_id = t.trip_id and speed > 55 and route_id = 71 and date(tstamp) = '2020-10-12'")
    rows = cur.fetchall()
    print("The row count for query 5b: ", cur.rowcount)
    features = []
    for row in rows:
        #trip_id = row[0]
        speed = row[1]
        if speed == "" or speed is None:
            continue
        lat = row[2]
        longit = row[3]
        if lat == "" or lat is None or longit == "" or longit is None:
            continue
        #time = row[4]

        try:
            latitude, longitude = map(float, (lat, longit))
            features.append(
                Feature(
                    geometry = Point((longitude,latitude)),
                    properties = {
                        'speed': (int(speed))
                    }
                )
            )
        except ValueError:
            continue
    fname = "query5b.geojson"
    result_to_geojson(features, fname)
    cur.close()  


def query5c(conn):
    """
    Busiest route of Oregon between 6 AM and 8 AM on some weekday (say Wednesday Oct 14, 2020).
    """

    cur = conn.cursor()  
    cur.execute("DROP VIEW IF EXISTS busy_route")
    cur.execute("create view busy_route as select count(*) as count, route_id from breadcrumb b, trip t where t.trip_id =b.trip_id and route_id IS NOT NULL and tstamp::time between time '06:00:00' and '08:00:00' and date(tstamp) = '2020-10-14' group by route_id order by count desc limit 1")
    cur.execute("select b.trip_id, speed, latitude, longitude from breadcrumb b, trip t, busy_route r where b.trip_id =t.trip_id and t.route_id = r.route_id and date(tstamp) = '2020-10-14' and tstamp::time between time '06:00:00' and '08:00:00'")
    rows = cur.fetchall()
    print("The row count for query 5c: ", cur.rowcount)
    features = []
    for row in rows:
        #trip_id = row[0]
        speed = row[1]
        if speed == "" or speed is None:
            continue
        lat = row[2]
        longit = row[3]
        if lat == "" or lat is None or longit == "" or longit is None:
            continue
        #time = row[4]

        try:
            latitude, longitude = map(float, (lat, longit))
            features.append(
                Feature(
                    geometry = Point((longitude,latitude)),
                    properties = {
                        'speed': (int(speed))
                    }
                )
            )
        except ValueError:
            continue
    fname = "query5c.geojson"
    result_to_geojson(features, fname)
    cur.close()  
    
def query5d(conn):
    """
     Busiest hour on Route 37 on Oct 14,2020.
    """

    cur = conn.cursor()
    time_frame = [['06:00:00','08:00:00'], ['08:00:01', '10:00:00'],['16:00:00','18:00:00'],['18:00:01','20:00:00']]
    count = []
    for time in time_frame:
        cur = conn.cursor()
        cur.execute("DROP VIEW IF EXISTS busy_hour")
        query = "create view busy_hour as select count(*) as count from trip t,breadcrumb b \
        where b.trip_id=t.trip_id and date(tstamp) = '2020-10-14' and route_id = 37 and \
        tstamp::time between time '"+time[0]+"' and '"+time[1]+"'"
        cur.execute(query)
        
        cur.execute("select * from busy_hour")
        rows1 = cur.fetchall()
        for r in rows1:
            count_cur = r[0]
            count.append(count_cur)
            max1 = max(count)
            if(count_cur < max1):
                pass
            else:
                query1 = "select t.trip_id, b.speed, b.latitude, b.longitude from breadcrumb b, trip t \
                where b.trip_id = t.trip_id and route_id = 37 and date(tstamp) = '2020-10-14' and \
                tstamp::time between time '"+time[0]+"' and '"+time[1]+"'"
                cur.execute(query1)
                rows = cur.fetchall()
    features = []
    for row in rows:
        #trip_id = row[0]
        speed = row[1]
        if speed == "" or speed is None:
            continue
        lat = row[2]
        longit = row[3]
        if lat == "" or lat is None or longit == "" or longit is None:
            continue
        try:
            latitude, longitude = map(float, (lat, longit))
            features.append(
                Feature(
                    geometry = Point((longitude,latitude)),
                    properties = {
                        'speed': (int(speed))
                    }
                )
            )
        except ValueError:
            continue
    fname = "query5d.geojson"
    result_to_geojson(features, fname)
    cur.close()    
  
    
def main():
    conn = dbconnect()
    query1(conn)
    query2(conn)
    query3(conn)
    query4(conn)
    query5a(conn)
    query5b(conn)
    query5c(conn)
    query5d(conn)
    
if __name__ == "__main__":
    main()
