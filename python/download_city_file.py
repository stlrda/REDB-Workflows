# Downloads a file from Saint Louis City Open Data Portal
# Must be passed an http:// or https:// file url string and a destination directory string

import pandas as pd
import wget
import datetime
import io

def download_city_file(file_url,destination_directory):
    print('Downloading file: ',file_url)
    print('To location: ',destination_directory)
    start_time = datetime.datetime.now()
    print('Starting at: ',start_time.strftime("%a, %b %d, %Y %I:%M:%S %p"))
    wget.download(file_url,destination_directory)
    end_time = datetime.datetime.now()
    print('Finished at: ',end_time.strftime("%I:%M:%S %p"))

#
# for item in city_files:
#     print(item)
