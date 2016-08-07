#!/home/ubuntu/one_bus/onebus/bin/python
# coding: utf-8

# ####*What do I want to do?*
# - Predict accuracy of given estimated time by:
#     - routeShortName
#     - busNumber
#     - scheduledDepartureTime
#     - predictedArrivalTime
# - Send alert when specified bus is within a specified time and there is live data on the departure based on array of stop/bus #s
#     - start out with the server side, write back end in python
#     - write android app possibly, or have it text through Hangouts API. 
# - Predict delays via DEBUGrmation from RSS feeds
# - Use sqlite3 database / pandas / scikit-learn to make more accurate predictions over time. 
# - **and...** other stuff. Need to figure that out. 

# questusersdb.ce1rtthkhn4v.us-east-1.rds.amazonaws.com
# db: one_bus


import os
import sys
import requests
from requests import exceptions as excs
import json
import datetime
import pandas as pd
import time
import datetime
import sqlalchemy
import logging
import random
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from sqlalchemy import Column, Integer, Unicode, UnicodeText, String, DateTime, Float
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.declarative import declarative_base
engine = create_engine('mysql://awsmaster:awsmaster@questusersdb.ce1rtthkhn4v.us-east-1.rds.amazonaws.com/one_bus', echo=True)

API_KEY='5eb17cbd-3e96-471a-803a-d4dcaf1a8e60'
WHERE_API='http://api.pugetsound.onebusaway.org/api/where'
logger = logging.getLogger('sqlalchemy')
logger.setLevel(logging.DEBUG)
fh = logging.FileHandler('./logs/{0}.log'.format(datetime.datetime.now().strftime('OneBusDataScraper_%m-%d-%y_%H-%M-%S')))
fh.setLevel(logging.DEBUG)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
fh.setFormatter(formatter)
logger.addHandler(fh)

# In[2]:

key_param_dict = {'key': API_KEY }


def get_me_my_departuretimes(stop_number):
    """:params: 
        stop_number: number of desired stop
        bus_lines: list of bus lines you want to query
        acceptable timerange: time range (in minutes) 
        :returns: dictionary of the intersection of these values"""
    try:
        request = requests.get('{0}/arrivals-and-departures-for-stop/1_{1}.json'.format(WHERE_API, stop_number), params=key_param_dict)
    except (excs.ConnectionError, excs.HTTPError, excs.Timeout) as err:
        logger.error('There was an issue with sending the http request to the API service. {}'.format(err.args))
        raise ConnectionError
    else:
        arrival_dictionary = json.loads(request.text)
        if "No such stop:" in arrival_dictionary.get('text'):
            raise KeyError
        # this is not great 
        ugh = arrival_dictionary['data']['entry']['arrivalsAndDepartures']
    return ugh


class OneBus:
    def __init__(self, trip_id, route_short_name, route_id, vehicle_id):
        self.trip_id = trip_id
        self.route_short_name = route_short_name
        self.route_id = route_id
        self.vehicle_id = vehicle_id
        ## this is what i'm thinking. objects represent one bus taking one route.
        # take note of their locations, and their arrivals as they occur. 
        # use sqlalchemy or redis
        # sqlalchemy : http://stackoverflow.com/questions/2047814/is-it-possible-to-store-python-class-objects-in-sqlite
        
        self.last_loc = []
        self.scheduled_arrival = []
        self.predicted_arrival = [] 
        # need more of the json. ss
    
def flatten_trip_status(dataframe):
    # TODO abstract this. it could easily be for any column that needs to be flattene
    logger.info('flattening trip status')
    if len(dataframe['tripStatus']) == 1:
        trip_status = pd.io.json.json_normalize(dataframe['tripStatus'])
    else:
        flattened_statuses = []
        for trip in dataframe['tripStatus']:
            flattened_statuses.append(pd.io.json.json_normalize(trip))
        trip_status = pd.concat(flattened_statuses)
    return trip_status


def load_up_database(dataframe):
    trip_status = flatten_trip_status(dataframe)
    trip_status_removed = dataframe.drop(['tripStatus'], axis=1)
    try:
        trip_status_removed.to_sql('one_bus_api_data', engine, if_exists='append', index_label='pd_index')
        renamed_trip_status = trip_status.rename(columns={
                                                          "position.lat": "position_lat",
                                                          "position.lon": "position_lon",
                                                          "lastKnownLocation.lat": "lastKnownLocation_lat",
                                                          "lastKnownLocation.lon": "lastKnownLocation_lon"
                                                         }
                                                )
        renamed_trip_status.drop('situationIds', axis=1).to_sql('trip_status', engine, if_exists='append', index_label='pd_index')
    except sqlalchemy.exc.OperationalError:
        logger.error("No database connection!!")
        sys.exit(1)
        
    

def is_in_rush_hour(datetime_obj):
    # Return the day of the week as an integer, where Monday is 0 and Sunday is 6.
    # if it's monday-friday:
    if datetime_obj.weekday() < 5:
        if (5 < datetime_obj.hour < 9) or (16 < datetime_obj.hour < 19):
            return True
    return False

def get_random_list(included_vals, range_val):
    full_random_list = [included_vals]
    for _ in range(range_val):
        random_number_string = str(random.randrange(1,10000))
        if random_number_string not in full_random_list:
            full_random_list.append(random_number_string)
    return full_random_list


def run(bus_stops):
    # multiple bus stops. array of 20 stops is randomly generated, but bus stops are included 
    s = requests.Session()
    s.mount('http://', requests.adapters.HTTPAdapter(max_retries=5))
    try:
        while True:
            current_time = datetime.datetime.now()
            logger.info(current_time.strftime('Time before sleep: %m/%d/%y %H:%M'))
            if is_in_rush_hour(current_time):
                time_sleep = 60
            else:
                time_sleep = 1
            logger.info('Sleeping for {0} s'.format(time_sleep))
            time.sleep(time_sleep)
            selected_stops = get_random_list(bus_stops, 20)
            logger.info('Choice + randomly selected bus stops: \n{0}'.format(selected_stops))
            dataframes = []
            for bus_stop in selected_stops:
                try:
                    # print('reading json')
                    # print("JSON---", get_me_my_departuretimes(bus_stop))
                    departures = get_me_my_departuretimes(bus_stop)
                    logger.info('JSON to be added for {0}: \n{1}'.format(bus_stop, json.dumps(departures, indent=4)))
                    departure_dataframe = pd.DataFrame(departures)
                except (KeyError, ConnectionError):
                    logger.error("Bus Stop #{0} could not be found".format(bus_stop))
                else:
                    try:
                        logger.info('Pruning dataframe for {0}'.format(bus_stop))
                        pruned = departure_dataframe.drop(['frequency', 'predictedDepartureInterval', 'tripHeadsign', 'situationIds'], axis=1)
                    except ValueError:
                        logger.error("Could not remove necessary columns from dataframe!!!")
                        logger.error(departure_dataframe.columns)
                    else:
                        dataframes.append(pruned)
            
            try:
                logger.info('concatenating dataframes')
                all_frames = pd.concat(dataframes)
            except ValueError as e:
                logger.error("Error concatenating!: %s" % e.args)
            else:
                logger.info('Inserting concatenated dataframes into database.')
                load_up_database(all_frames)            
    except KeyboardInterrupt:
        logger.error('broken by control c')
        sys.exit()

def send_unhandled_error_email(e):

    # Open a plain text file for reading.  For this example, assume that
    # the text file contains only ASCII characters.
    fromaddr = "jessishank1@gmail.com"
    toaddr = "jessishank1@gmail.com"
    # Create a text/plain message
    msg = MIMEMultipart()
    msg['From'] = fromaddr
    msg['To'] = toaddr
    msg['Subject'] = 'Unhandled Error from OneBus script!'
    # could be better.
    body = '\n'.join([error for error in e.args])
    msg.attach(MIMEText(body, 'plain'))
    server = smtplib.SMTP('smtp.gmail.com', 587)
    server.starttls()
    server.login(fromaddr, 'chocolateeggs')
    text = msg.as_string()
    server.sendmail(fromaddr, toaddr, text)
    # s.sendmail(me, [you], msg.as_string())
    server.quit()


if __name__=='__main__':
    try:
        run('6050')
    except Exception as e:
        send_unhandled_error_email(e)
        sys.exit()


