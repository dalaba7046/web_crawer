import time
import sys
from math import *
import numpy as np
from datetime import datetime
import pymysql
from sqlalchemy import create_engine
from bs4 import BeautifulSoup as bs
import requests,json

def np_getDistance(A , B ):# 先緯度後經度
    ra = 6378140  # radius of equator: meter
    rb = 6356755  # radius of polar: meter
    flatten = 0.003353 # Partial rate of the earth
    # change angle to radians
    
    radLatA = np.radians(A[:,0])
    radLonA = np.radians(A[:,1])
    radLatB = np.radians(B[:,0])
    radLonB = np.radians(B[:,1])
 
    pA = np.arctan(rb / ra * np.tan(radLatA))
    pB = np.arctan(rb / ra * np.tan(radLatB))
    
    x = np.arccos( np.multiply(np.sin(pA),np.sin(pB)) + np.multiply(np.multiply(np.cos(pA),np.cos(pB)),np.cos(radLonA - radLonB)))
    c1 = np.multiply((np.sin(x) - x) , np.power((np.sin(pA) + np.sin(pB)),2)) / np.power(np.cos(x / 2),2)
    c2 = np.multiply((np.sin(x) + x) , np.power((np.sin(pA) - np.sin(pB)),2)) / np.power(np.sin(x / 2),2)
    dr = flatten / 8 * (c1 - c2)
    distance = 0.001 * ra * (x + dr)
    return distance

#資料庫連線
engine2=create_engine('mysql+pymysql://admin:Pn123456@192.168.1.124:3306/cwb?charset=utf8')
con2 = engine2.connect()


con2.execute('select st_name from cwb_station_status').fetchall()
st_data=con2.execute('select st_name from cwb_station_status').fetchall()
st_cor=con2.execute('select latitude,longitude from cwb_station_status').fetchall()
#觀測站座標
loltALL_center=[]
#觀測站名稱
nameALL=[]
for i in range(len(st_data)):
	nameALL.append(st_data[i][0])
for j in range(len(st_data)):
    loltALL_center.append(list(st_cor[j]))
for k in range(len(loltALL_center)):
    loltALL_center[k][0]=float(loltALL_center[k][0])
    loltALL_center[k][1]=float(loltALL_center[k][1])
loltALL_center=np.matrix(loltALL_center)




land_latitude=input('請輸入緯度:')
land_longitude=input('請輸入經度:')
while land_latitude=='':
    print('請重新輸入')
    land_latitude=input('請輸入緯度:')
while land_longitude=='':
    print('請重新輸入')
    land_longitude=input('請輸入經度:')
land_latitude=float(land_latitude)
land_longitude=float(land_longitude)
Find_loc = np.matrix([[land_latitude,land_longitude]])

#開始時間
starttime = datetime.now()
# 距觀測站距離(全部)
disALL = np_getDistance(loltALL_center,Find_loc)
# 距離各觀測站最短距離(全部)
disMinName = nameALL[int(disALL.argmin(axis=0))]
disMinDistance = floor(disALL[int(disALL.argmin(axis=0))])
#結束時間
endtime = datetime.now()

print("離 %s 觀測站最近, %1.1f 公里"%(disMinName,disMinDistance))
print("use %s usec"%(endtime-starttime).microseconds)
try:
    #自動氣象站
    url='https://opendata.cwb.gov.tw/api/v1/rest/datastore/O-A0001-001?Authorization=rdec-key-123-45678-011121314&locationName='+disMinName
    html=requests.get(url)
    task=html.json()
    
    location=task['records']['location'][0]['locationName']
    latitude=task['records']['location'][0]['lat']
    longitude=task['records']['location'][0]['lon']
    station_id=task['records']['location'][0]['stationId']
    temperature=str(task['records']['location'][0]['weatherElement'][3]['elementValue'])
    humidity=str(task['records']['location'][0]['weatherElement'][4]['elementValue'])
    rainfull=str(task['records']['location'][0]['weatherElement'][6]['elementValue'])
    speed=str(task['records']['location'][0]['weatherElement'][2]['elementValue'])
    direction=str(task['records']['location'][0]['weatherElement'][1]['elementValue'])
    weather_data=(
      '觀測站地點:'+str(location)+
      '\n觀測站代碼'+station_id+
      '\n經度:'+str(longitude)+
      '\n緯度:'+str(latitude)+
      '\n溫度(ºC):'+temperature+
      '\n濕度(%):'+humidity+
      '\n降雨量(mm):'+rainfull+
      '\n最大風速(m/s):'+speed+
      '\n平均風向(度):'+direction
      )
    print(weather_data)

except:
    #局屬氣象站
    url='https://opendata.cwb.gov.tw/api/v1/rest/datastore/O-A0003-001?Authorization=rdec-key-123-45678-011121314&locationName='+disMinName

    html=requests.get(url)
    task=html.json()
    
    location=task['records']['location'][0]['locationName']
    latitude=task['records']['location'][0]['lat']
    longitude=task['records']['location'][0]['lon']
    station_id=task['records']['location'][0]['stationId']
    temperature=str(task['records']['location'][0]['weatherElement'][3]['elementValue'])
    humidity=str(task['records']['location'][0]['weatherElement'][4]['elementValue'])
    rainfull=str(task['records']['location'][0]['weatherElement'][6]['elementValue'])
    ten_win_speed=str(task['records']['location'][0]['weatherElement'][10]['elementValue'])
    ten_wind_direction=str(task['records']['location'][0]['weatherElement'][11]['elementValue'])
    weather_data=(
          '觀測站地點:'+str(location)+
          '\n觀測站代碼'+station_id+
          '\n經度:'+str(longitude)+
          '\n緯度:'+str(latitude)+
          '\n溫度(ºC):'+temperature+
          '\n濕度(%):'+humidity+
          '\n降雨量(mm):'+rainfull+
          '\n10分鐘內最大風速(m/s):'+ten_win_speed+
          '\n10分鐘內平均風向(度):'+ten_wind_direction
          )
    print(weather_data)
con2.close()
   
    
