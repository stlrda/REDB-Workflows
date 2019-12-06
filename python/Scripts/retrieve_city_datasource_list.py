import urllib.request

city_datasource_list_url = 'https://raw.githubusercontent.com/stlrda/redb_python/master/reference/redb_source_databases_all-info.csv' # TODO point this to the same file in /config, and delete the one in reference
working_directory = '/Users/jonathanleek/Desktop/working' # TODO modify retrieve_city_datasource_list to utilize s3 bucket

def retrieve_city_datasource_list():
    print('Downloading list of city datasources...')
    urllib.request.urlretrieve(city_datasource_list_url, working_directory+'/city_datasource_list.csv')

#retrieve_city_datasource_list()