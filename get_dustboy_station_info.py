import requests
import pandas as pd
import pandas_gbq
from google.oauth2.service_account import Credentials
import requests
from bs4 import BeautifulSoup
import numpy as np
import datetime

url = 'https://www.cmuccdc.org/api/ccdc/stations'
resp = requests.get(url)
station_data = resp.json()
station_data = pd.json_normalize(station_data) 

station_data.drop(['dustboy_alias', 'dustboy_pv', 'dustboy_version', 'db_email', 'db_co', 'db_mobile',
       'db_addr', 'db_status', 'db_model'], axis=1, inplace=True)
station_data.to_excel('dustboy_station_info.xlsx', index=False)