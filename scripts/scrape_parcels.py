# Function for Scraping the Parcel API and Storing in The RedB Database
# Needs to be converted into an Airflow DAG and Ran on First of Month

import requests
import psycopg2
from psycopg2.extras import Json, DictCursor

# These Secrets are Already Available to Airflow in the RedB Connector
# See https://github.com/stlrda/REDB-Workflows/blob/master/dags/REDB_ELT.py#L26
DB_HOST = ''
DB_NAME = 'redb'
DB_PORT = 5432
DB_PASS = ''
DB_USER = 'airflow_user'

API_KEY = ''
# API_Key needs to be made into an Airflow Connection


# Connection to DB (Will get replaced with Airflow Connector)
conn = psycopg2.connect(host = DB_HOST, port = DB_PORT, user = DB_USER, password = DB_PASS, database = DB_NAME)

def api_get_parcel(url, key, handle):
    query = url + '?key=' + key + '&handle=' + handle
    try:
        resp = requests.get(query)
        data = resp.json()
    except Exception:
        print('API Failure at Handle: ' + handle)
        data = '{"No": "Data"}'
    return data

def scrape_parcel_api(url, key, list_handles):
    for handle in list_handles:
        # Get Parcel Info from API
        try:
            parcel_info = api_get_parcel(url, key, handle)
            print(handle + ' Returned Data')
        except Exception:
            print('Failure to get Data at:' + handle)

        # Put this Info in the Database
        cursor = conn.cursor(cursor_factory=DictCursor)
        try:
            cursor.execute("INSERT INTO city_api.parcel_data (handle, parcel_data) VALUES(%s, %s) ON CONFLICT (handle) DO UPDATE SET parcel_data = %s", (handle, Json(parcel_info), Json(parcel_info)))
            conn.commit()
        except Exception:
            print("Could not insert parcel")
        cursor.close()

# Get List of Handles
# This needs to be changed to either:
#  A. (Download/extract a list from the city, see https://www.stlouis-mo.gov/data/datasets/dataset.cfm?id=82)
#  B. (SELECT handle FROM city_api.parcel_data)

with open('handles.csv', 'r') as file:
    handle_list = file.read()
handle_list = handle_list.split('\n')[:-1]

scrape_parcel_api('https://portalcw.stlouis-mo.gov/a/property', API_KEY, handle_list)
