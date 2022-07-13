import datetime
import time 
import requests
import json
import pandas as pd
import os 
from pymongo import MongoClient
from urllib.parse import quote_plus

#reading auth data
json_path = "/home/pi/Desktop/python/pollution/aqi/sensor/keys.json"

with open(json_path, 'r') as f: 
    auth_keys = json.load(f)
    

'''
#I need it just to get the refresh token for the first time
auth_url = "https://www.strava.com/api/v3/oauth/token"
PARAMS = {"client_id" : auth_keys["strava_client_id"], "client_secret" : auth_keys["strava_client_secret"], "code" : auth_keys["strava_auth_code"], "grant_type" : "authorization_code"}   
auth_resp = requests.post(url = auth_url, params = PARAMS)
auth_cred = auth_resp.json()
access_token = auth_cred["access_token"]
refress_token = auth_cred["refresh_token"]
'''

#generating new access and refresh tokens
PARAMS = {"client_id" : auth_keys["strava_client_id"], "client_secret" : auth_keys["strava_client_secret"], "refresh_token" : auth_keys["strava_refresh_token"], "grant_type" : "refresh_token"}   
auth_url = "https://www.strava.com/oauth/token"

auth_resp = requests.post(url = auth_url, params = PARAMS)
print("Auth: {}".format(auth_resp.status_code))
request_data = auth_resp.json()

access_token = request_data["access_token"]
refress_token = request_data["refresh_token"]
auth_keys["strava_refresh_token"] = refress_token
auth_keys["strava_access_token"] = access_token

#save the new tokens
with open(json_path, 'w') as f:
    f.write(json.dumps(auth_keys))

print("writing back auth keys.")

#################################

#Finding Activity

activity_id = "7445907547"

activity_url = "https://www.strava.com/api/v3/activities/" + activity_id
activity_param = {"access_token" : access_token, "include_all_efforts" : False}

activity_res = requests.get(url = activity_url, params = activity_param)

print("Finding activity! Status: {}".format(activity_res.status_code))

activity = activity_res.json()

ride_name = activity["name"]
start_date = activity["start_date_local"].replace('Z', '')
st = datetime.datetime.fromisoformat(start_date)
start_latlng = activity["start_latlng"]
end_latlng = activity["end_latlng"]

#################################
#Finding Stream

stream_url = "https://www.strava.com/api/v3/activities/" + activity_id + "/streams"
stream_param = {"keys" : "distance,time,latlng", "key_by_type" : True, "access_token" : access_token}


stream_res = requests.get(url = stream_url, params = stream_param)
print("Finding stream! Status: {}".format(stream_res.status_code))

print("Creating dataframe...")

stream = stream_res.json()
latlng = stream["latlng"]["data"]
times = stream["time"]["data"]

data = {"latlng" : latlng, "time" : times}
df = pd.DataFrame(data)

#split lat long data into 2 columns
df[["latitude", "longitude"]] = pd.DataFrame(df.latlng.tolist())

df["start_date"] = start_date
df["start_date_unix"] = st.timestamp()
df["start_latlng"] = str(start_latlng)
df["end_latlng"] = str(end_latlng)
df["ride_name"] = ride_name

def add_time(row):
    return row["start_date_unix"] + row["time"]

df["actual_timestamp_unix"] = df.apply(add_time, axis = 1)
df["actual_timestamp_unix"] = df["actual_timestamp_unix"].astype(int)
df = df.drop("latlng", axis = 1)

print("Dataframe is ready!")

#################################
#Saving data to csv

csv_name = ride_name + ".csv"

df.to_csv("/home/pi/Desktop/python/pollution/aqi/sensor/data/" + csv_name)
print("csv: {} saved to data folder.".format(csv_name))

#Saving data to mongodb
df = df.drop(['time', 'start_date_unix', 'start_latlng', 'end_latlng'], axis=1)
columns_sorted = ['ride_name', 'start_date', 'actual_timestamp_unix', 'latitude', 'longitude']

df_mongodb = df[columns_sorted]
df_dict = df_mongodb.to_dict('records')

#log in to mongodb
user = quote_plus(auth_keys["mongo_user"])
pw = quote_plus(auth_keys["mongo_pw"])

uri = 'mongodb://%s:%s@127.0.0.1:27017'%(user, pw)
client = MongoClient(uri)
db = client.aqi

gps_db = db.gps_activity.insert_many(df_dict)
print("Inserting to mongodb. {}".format(gps_db))
 

