import fastf1
import pandas as pd
from kafka import KafkaProducer
from time import sleep
from json import dumps
import json
from datetime import datetime
import threading


def getData(driver):
  fastf1.Cache.enable_cache('f1cache')
  session = fastf1.get_session(2021, 'Abu Dhabi Grand Prix', 'R')
  session.load(laps=True, telemetry=True, messages=False, livedata=None)

  obj = session.laps.pick_driver(driver)

  tel = pd.DataFrame()

  for ind, lap in obj.iterlaps():
    ctel = lap.get_car_data()
    ctel.fill_missing()
    ctel["X"] = lap.telemetry["X"]
    ctel["Y"] = lap.telemetry["Y"]
    ctel["Lap"] = lap.LapNumber
    ctel["Driver"] = lap.Driver
    ctel["Team"] = lap.Team
    
    tel = pd.concat([tel,ctel])
    

  tel["Time"] = tel['Time'].dt.total_seconds()
  tel.drop("SessionTime", inplace=True, axis=1)
  tel.drop("Date", inplace=True, axis=1)

  teld = tel.to_dict('records')
  


  for row in teld:
    producer.send('f1telemetry', value=row)
    producer.flush()
    sleep(0.240)


 
producer = KafkaProducer(bootstrap_servers=['localhost:9092'], value_serializer=lambda x: dumps(x).encode('utf-8'))

threading.Thread(target=getData, args = ('VER',)).start()
threading.Thread(target=getData, args = ('HAM',)).start()