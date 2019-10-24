# Downloads a file from Saint Louis City Open Data Portal
# Must be passed an http:// or https:// file url string and a destination directory string

import pandas as pd
import wget
import io

def download_city_file(file_url,destination_directory):
    print('Downloading file: ',file_url)
    print('To location: ',destination_directory)
    wget.download(file_url,destination_directory)

#
# for item in city_files:
#     print(item)
