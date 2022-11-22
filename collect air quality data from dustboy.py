import requests
import pandas as pd
import pandas_gbq
from google.oauth2.service_account import Credentials
import requests
from bs4 import BeautifulSoup
import numpy as np
import datetime

def get_dustboy_url():
    url = "https://www.cmuccdc.org/download"
    reqs = requests.get(url)
    soup = BeautifulSoup(reqs.text, 'html.parser')
    station = soup.find_all('div',{'class':'station_list'})
    list_url = []
    for links in station:
        list_url.append(links.a['href'])

    link_data = pd.DataFrame([list_url])
    link_data =link_data.transpose()
    link_data.columns = ['URL']
    return link_data

def get_dustboy_historical_data(link):
    final_data = pd.DataFrame()
    for url in link['URL']:
        
        try:
            print(url)
            resp = requests.get(f'{url}')
            hist_data = resp.json()
            hist_data = pd.json_normalize(hist_data) 
            data = pd.json_normalize(hist_data['value'])
            data.transpose
            N_row, N_col = np.shape(data)
            pm10 = []
            pm2_5 = []
            date = []
            Lat = []
            Long = []
            Station_ID = []
            for i in range(N_col):
                date.append(data[i][0]['log_datetime'])
                pm2_5.append(data[i][0]['pm25'])
                pm10.append(data[i][0]['pm10'])
                Station_ID.append(int(hist_data['id']))
                Lat.append(float(hist_data['dustboy_lat']))
                Long.append(float(hist_data['dustboy_lon']))
            df = pd.DataFrame(list(zip(date, pm2_5,pm10,Station_ID,Lat,Long)), columns =['Date_time', 'PM2_5','PM10',
                                                                                'Station_ID','Lat','Long']) 
            df['Date_time'] = pd.to_datetime(df['Date_time'], utc = True)
            df['Date'] = df['Date_time'].dt.date
            df['Time'] = df['Date_time'].dt.time
            df['Source'] = 'dustboy'
            df = df[['Date','Time','PM2_5','PM10','Source','Station_ID','Lat','Long']]
            
            
        except Exception as err:
            print(err)
        final_data = pd.concat([final_data, df], axis=0)
    return final_data

link = get_dustboy_url()
data = get_dustboy_historical_data(link)
data.to_csv('dustboy_data.csv', index=False)