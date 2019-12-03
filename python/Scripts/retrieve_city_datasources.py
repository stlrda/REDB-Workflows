import pandas as pd
import urllib.request

working_directory = '/Users/jonathanleek/Desktop/working'

def retrieve_city_datasources():
    df = pd.read_csv(working_directory+'/city_datasource_list.csv')
    targets = df['Direct URL']
    targets = list(set(targets))
    for target in targets:
        print("Retieving " + target)
        urllib.request.urlretrieve(target, working_directory + "/" + target.rsplit('/', 1)[-1])
        print("Downloaded "+target.rsplit('/', 1)[-1])
retrieve_city_datasources()