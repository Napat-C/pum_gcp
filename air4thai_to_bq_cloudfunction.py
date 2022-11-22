import base64
import requests
import pandas as pd
from pandas.io import gbq

def api_to_df(url):
  data = requests.get(url)
  json = data.json()
  df = pd.json_normalize(json['stations'])
  cols = df.columns
  cols=cols.str.replace('LastUpdate.', '',regex = True)
  cols=cols.str.replace('.value', '',regex = True)
  cols=cols.str.replace('.aqi', '',regex = True)
  cols=cols.str.replace('AQI.Level', 'AQI_Level',regex = True)
  df.columns=cols
  df.rename(columns = {'stationID':'Station_ID','date':'Date', 'time':'Time','lat':'Lat','long':'Long','PM25':'PM2_5'}, inplace = True)
  df['Source'] = 'Air4Thai'
  data_out = df[['Date','Time','PM2_5','PM10','Source','Station_ID','Lat','Long']]
  return data_out

def main(data, context):
  df = api_to_df('http://air4thai.pcd.go.th/services/getNewAQI_JSON.php')

  df.to_gbq(destination_table='gsheet-364411.aqi_air4thai.aqi',project_id='gsheet-364411',
            if_exists='append')