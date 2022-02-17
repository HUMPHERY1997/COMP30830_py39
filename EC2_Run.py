#!/usr/bin/env python
# coding: utf-8

# In[1]:


import requests
import json
import time
import traceback
import pymysql
import pandas as pd
from datetime import datetime


# CONNECT TO AWS RDS

# In[2]:


dbikes = pymysql.Connect(
    host = "dbikes.ccike2q3zkya.eu-west-1.rds.amazonaws.com",
    user = "admin",
    passwd = "admin2022",
    database = "dbikes")


# CREATE THREE TABLES
# 
# static_station columns: number, name, bike stands, lat, lng
# 
# dynamic_station columns: number, last_update, available_bike_stands, available_bikes
# 
# dynamic_weather columns: number, lat, lng, dt, temp, pressure, wind_speed, humidity

# In[3]:


cur = dbikes.cursor()
cur.execute("CREATE TABLE IF NOT EXISTS static_station(number integer, name varchar(45), bike_stands integer, lat float, lng float)")
cur.execute("CREATE TABLE IF NOT EXISTS dynamic_station(number integer, last_update integer, available_bike_stands integer, available_bikes integer)")
cur.execute("CREATE TABLE IF NOT EXISTS dynamic_weather(number integer, lat float, lng float, dt integer, temp float, pressure float, wind_speed float, humidity float)")


# Insert the static_station table

# In[9]:


# for i in range(len(data)):
#     station_row = data[i]
#     static_station_insert_query = "INSERT INTO static_station(number, name, bike_stands, lat, lng) VALUES(%s, %s, %s, %s, %s)"
#     static_station_data = (int(station_row.get("number")),
#                            station_row.get("name"),
#                            int(station_row.get("bike_stands")),
#                            float(station_row.get("position").get("lat")),
#                            float(station_row.get("position").get("lng")))
#     cur.execute(static_station_insert_query, static_station_data)
# dbikes.commit()           


# Method to request data from JDCecaux API and Openweather API

# In[4]:


#dublin bike station api
APIKEY = "a4ae2329e4585bbd13dcf83332d04b69a88fb904" 
NAME = "Dublin" 
STATIONS_URI = "https://api.jcdecaux.com/vls/v1/stations"
api_response = requests.get(STATIONS_URI, params={"apiKey": APIKEY, "contract": NAME})
data = json.loads(api_response.text)

#openweather api
WEATHER_APIKEY = "eeec159d31a816c2152dbf05ba6e0076"
WEATHER_URL = "http://api.openweathermap.org/data/2.5/weather?lat={}&lon={}&appid={}"


# In[14]:


def InsertData2Tables(station):
    for i in range(0,len(station)):
        station_row = station[i]
        weather_response = requests.get(WEATHER_URL.format(station_row.get("position").get("lat"), station_row.get("position").get("lng"), WEATHER_APIKEY))
        weather = json.loads(weather_response.text)
#         insert the station data
        station_insert_query = "INSERT INTO dynamic_station(number, last_update, available_bike_stands, available_bikes) VALUES(%s, %s, %s, %s)"
        station_data = (int(station_row.get("number")),
                        float(station_row.get("last_update")),
                        int(station_row.get("available_bike_stands")),
                        int(station_row.get("available_bikes")))
        cur.execute(station_insert_query, station_data)
        
#         insert the weather data
#         here still in this rage because each station match each weather, which is the same in number
        weather_insert_query = "INSERT INTO dynamic_weather(number, lat, lng, dt, temp, pressure, wind_speed, humidity) VALUES(%s, %s, %s, %s, %s, %s, %s, %s)"
        weather_data = (int(station_row.get("number")),
                        float(weather["coord"].get("lat")),
                        float(weather["coord"].get("lon")),
                        int(weather["dt"]),
                        float(weather["main"].get("temp")),
                        float(weather["main"].get("pressure")),
                        float(weather["wind"].get("speed")),
                        float(weather["main"].get("humidity")))
        cur.execute(weather_insert_query, weather_data)
    dbikes.commit()


# In[15]:


def ContinuousGetData():
    while True:
        try:
            api_response = requests.get(STATIONS_URI, params={"apiKey": APIKEY, "contract": NAME})
            data = json.loads(api_response.text)
            InsertData2Tables(data)
            
#             sleep every 5 minutes
            time.sleep(5*60)
    
        except:
#             hit for problems
            print(traceback.format_exc())


# In[ ]:


ContinuousGetData()

