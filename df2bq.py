import requests
import pandas as pd
import pandas_gbq
from google.oauth2.service_account import Credentials


def get_data():
    data = requests.get('http://air4thai.pcd.go.th/services/getNewAQI_JSON.php')
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
    data = df[['Date','Time','PM2_5','PM10','Source','Station_ID','Lat','Long']]
    return data

"""
 Please create dataset and table in google big query with follwing schema below 
'Date','Time','PM2_5','PM10','Source','Station_ID','Lat','Long' 
and all types are string
"""

data = get_data()
credential_file = "gsheet-364411-b07c5744ab35.json"
credential = Credentials.from_service_account_file(credential_file)
data.to_gbq(destination_table='gsheet-364411.aqi_air4thai.aqi',project_id='gsheet-364411',
            if_exists='append',credentials=credential)