import json
from datetime import date

from urllib.request import urlopen
from bs4 import BeautifulSoup

# the URL containing the BREADCRUMB data. Pass this URL to the
# urlopen() to get the response.
breadcrumb_url = 'http://rbi.ddns.net/getBreadCrumbData'
response = urlopen(breadcrumb_url)

# store the data in the json format
data = json.loads(response.read().decode('utf-8'))
current_date = date.today()
date = '/home/agrawal/examples/clients/cloud/python/sensor_data/' + str(current_date) + '.json'
with open(date, 'w') as file:
    json.dump(data, file, indent = 2)

# the URL containing the STOP EVENT data. Pass this URL to the 
# urlopen() to get the html of the page
stop_event_url = 'http://rbi.ddns.net/getStopEvents'
html = urlopen(stop_event_url)

# the BeautifulSoup is called to parse the html files. It takes the raw
# html text and break it into Python objects. The soup object allows you
# to extract interesting information about the website
soup = BeautifulSoup(html, 'lxml')
h3 = soup.find_all('h3') # find_all() will extract all the h3 headers 

trip_id_h3 = [] # Empty list to store all the h3 headers without html tags
for record in h3:
    str_h3 = str(record)
    # To remove html tags, pass the h3 string into BeautifulSoup() 
    # and use the get_text() method to extract the text without html tags.
    clean_text = BeautifulSoup(str_h3, 'lxml').get_text()
    trip_id_h3.append(clean_text)
    
trip_id = [] # parse h3 to get the trip_ids and store it in the trip_id list
for record in trip_id_h3:
    record_list = record.split(' ')
    trip_id_num = int(record_list[4])
    trip_id.append(trip_id_num)

# find_all() of soup object will extract all the tables
tables = soup.find_all('table')
stop_event = [] # Empty list to store each table record as a JSON string
for table in tables:
    trip = trip_id[0]
    trip_id = trip_id[1:]
    rows = table.find_all('tr')

    # Skipped the rows[0] to ignore the header tag <th>
    for row in rows[1:]:
        row_td = row.find_all('td')
        str_cells1 = str(row_td)
        clean_text = BeautifulSoup(str_cells1, 'lxml').get_text()
        
        # create a list from cleantext
        stop_event_rows = clean_text.split(', ')
        
        # strip '[' from first element of the list and ']' from the last element of the list
        stop_event_row = stop_event_rows[0].split('[')
        stop_event_rows[0] = stop_event_row[1]
        size = len(stop_event_rows)
        stop_event_row = stop_event_rows[size - 1].split(']')
        stop_event_rows[size - 1] = stop_event_row[0]

        # convert the stop_event_rows to JSON string and append it to the stop_event list
        for _ in range(len(stop_event_rows)):
            data = {}
            data['trip_id'] = trip
            data['vehicle_number'] = stop_event_rows[0]
            data['leave_time'] = stop_event_rows[1]
            data['train'] = stop_event_rows[2]            
            data['route_number'] = stop_event_rows[3]
            data['direction'] = stop_event_rows[4]
            data['service_key'] = stop_event_rows[5]
            data['stop_time'] = stop_event_rows[6]
            data['arrive_time'] = stop_event_rows[7]
            data['dwell'] = stop_event_rows[8]            
            data['location_id'] = stop_event_rows[9]
            data['door'] = stop_event_rows[10] 
            data['lift'] = stop_event_rows[11]
            data['ons'] = stop_event_rows[12]
            data['offs'] = stop_event_rows[13]           
            data['estimated_load'] = stop_event_rows[14]
            data['maximum_speed'] = stop_event_rows[15] 
            data['train_mileage'] = stop_event_rows[16]
            data['pattern_distance'] = stop_event_rows[17]
            data['location_distance'] = stop_event_rows[18]
            data['x_coordinate'] = stop_event_rows[19]            
            data['y_coordinate'] = stop_event_rows[20]
            data['data_source'] = stop_event_rows[21] 
            data['schedule_status'] = stop_event_rows[22] 
            
            json_str = json.dumps(data)
            data = json.loads(json_str)
            stop_event.append(data)

fname = '/home/agrawal/examples/clients/cloud/python/stop_event/' + str(current_date) + '.json'
with open(fname, 'w') as file:
    json.dump(stop_event, file, indent = 2)    