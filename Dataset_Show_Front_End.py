#!/usr/bin/env python
# coding: utf-8

# In[116]:


import requests
import json
import time
import traceback
import pymysql
import pandas as pd
from datetime import datetime


# In[117]:


front_end_dbikes = pymysql.Connect(
    host = "dbikes.ccike2q3zkya.eu-west-1.rds.amazonaws.com",
    user = "admin",
    passwd = "admin2022",
    database = "dbikes")


# In[118]:


frontend_cur = front_end_dbikes.cursor()
frontend_cur.execute("CREATE TABLE IF NOT EXISTS show_front_end(number integer, last_update integer, name varchar(45), bike_stands integer, lat float, lng float, available_bike_stands integer, available_bikes integer, weather varchar(45), temp float, pressure float, wind_speed float, humidity float)")


# In[119]:


#dublin bike station api
APIKEY = "a4ae2329e4585bbd13dcf83332d04b69a88fb904" 
NAME = "Dublin" 
STATIONS_URI = "https://api.jcdecaux.com/vls/v1/stations"
api_response = requests.get(STATIONS_URI, params={"apiKey": APIKEY, "contract": NAME})
data = json.loads(api_response.text)

#openweather api
WEATHER_APIKEY = "eeec159d31a816c2152dbf05ba6e0076"
WEATHER_URL = "http://api.openweathermap.org/data/2.5/weather?lat={}&lon={}&appid={}"


# In[120]:


def InsertData2Tables(station):
    for i in range(0,len(station)):
        station_row = station[i]
        weather_response = requests.get(WEATHER_URL.format(station_row.get("position").get("lat"), station_row.get("position").get("lng"), WEATHER_APIKEY))
        weather = json.loads(weather_response.text)
#         insert the station data
#         station_insert_query = "INSERT INTO show_front_end(number, last_update, available_bike_stands, available_bikes) VALUES(%s, %s, %s, %s)"
#         station_data = (int(station_row.get("number")),
#                         float(station_row.get("last_update")),
#                         int(station_row.get("available_bike_stands")),
#                         int(station_row.get("available_bikes")))
#         cur.execute(station_insert_query, station_data)
        
#         insert the weather data
#         here still in this rage because each station match each weather, which is the same in number
        front_end_insert_query = "INSERT INTO show_front_end(number, last_update, name, bike_stands, lat, lng, available_bike_stands, available_bikes, weather, temp, pressure, wind_speed, humidity) VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
        insert_data = ( int(station_row.get("number")),
                        float(station_row.get("last_update")),
                        str(station_row.get("name")),
                        int(station_row.get("bike_stands")),
                        float(weather["coord"].get("lat")),
                        float(weather["coord"].get("lon")),
                        int(station_row.get("available_bike_stands")),
                        int(station_row.get("available_bikes")),
                        str(weather["weather"][0].get("main")),
                        float(weather["main"].get("temp")),
                        float(weather["main"].get("pressure")),
                        float(weather["wind"].get("speed")),
                        float(weather["main"].get("humidity")))
        frontend_cur.execute(front_end_insert_query, insert_data)
    front_end_dbikes.commit()


# In[121]:


def drop_before_data():
    front_end_delete_query = "DELETE FROM show_front_end"
    frontend_cur.execute(front_end_delete_query)
    front_end_dbikes.commit()


# In[124]:


def ContinuousUpDateData():
    while True:
        try:
            drop_before_data()
            api_response = requests.get(STATIONS_URI, params={"apiKey": APIKEY, "contract": NAME})
            data = json.loads(api_response.text)
            InsertData2Tables(data)
#             sleep every 5 minutes
            time.sleep(5*60)
    
        except:
#             hit for problems
            print(traceback.format_exc())


# In[ ]:


ContinuousUpDateData()

