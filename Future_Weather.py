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


# In[2]:


future_dbikes = pymysql.Connect(
    host = "dbikes.ccike2q3zkya.eu-west-1.rds.amazonaws.com",
    user = "admin",
    passwd = "admin2022",
    database = "dbikes")


# In[3]:


future_cur = future_dbikes.cursor()
future_cur.execute("CREATE TABLE IF NOT EXISTS future_weather(number integer, dt integer, lat float, lng float, weather varchar(45), temp float, pressure float, wind_speed float, humidity float)")


# In[4]:


#dublin bike station api
APIKEY = "a4ae2329e4585bbd13dcf83332d04b69a88fb904" 
NAME = "Dublin" 
STATIONS_URI = "https://api.jcdecaux.com/vls/v1/stations"
api_response = requests.get(STATIONS_URI, params={"apiKey": APIKEY, "contract": NAME})
data = json.loads(api_response.text)

WEATHER_APIKEY = "eeec159d31a816c2152dbf05ba6e0076"
WEATHER_URL = "https://api.openweathermap.org/data/2.5/onecall?lat={}&lon={}&appid={}"


# In[5]:


def InsertData2Tables(station):
    for i in range(0,len(station)):
        station_row = station[i]
        weather_response = requests.get(WEATHER_URL.format(station_row.get("position").get("lat"), station_row.get("position").get("lng"), WEATHER_APIKEY))
        weather = json.loads(weather_response.text)
        for j in range(len(weather['hourly'])):
            future_insert_query = "INSERT INTO future_weather(number, dt, lat, lng, weather, temp, pressure, wind_speed, humidity) VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s)"
            insert_data = ( int(station_row.get("number")),
                            int(weather['hourly'][j]["dt"]),
                            float(station_row.get("position").get("lat")),
                            float(station_row.get("position").get("lng")),
                            str(weather['hourly'][j]["weather"][0]["main"]),
                            float(weather['hourly'][j]["temp"]),
                            float(weather['hourly'][j]["pressure"]),
                            float(weather['hourly'][j]["wind_speed"]),
                            float(weather['hourly'][j]["humidity"]))
            future_cur.execute(future_insert_query, insert_data)
            future_dbikes.commit()


# In[6]:


def drop_before_data():
    future_delete_query = "DELETE FROM future_weather"
    future_cur.execute(future_delete_query)
    future_dbikes.commit()


# In[7]:


def ContinuousUpDateData():
    while True:
        try:
            drop_before_data()
            api_response = requests.get(STATIONS_URI, params={"apiKey": APIKEY, "contract": NAME})
            data = json.loads(api_response.text)
            InsertData2Tables(data)
#             sleep every hour
            time.sleep(60*60)
    
        except:
#             hit for problems
            print(traceback.format_exc())


# In[ ]:


ContinuousUpDateData()

