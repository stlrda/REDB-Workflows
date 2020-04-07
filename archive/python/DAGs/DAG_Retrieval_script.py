from datetime import datetime, timedelta
import requests
import json



def DAG_Updater():

    DAGs = []

    responseAge = requests.get('https://api.github.com/repos/stlrda/redb_python/commits?path=/python/DAGs')
    dagAge = json.loads(responseAge.text)

    for data in dagAge:
        info  = {'age' : data['commit']['committer']['date']}
        DAGs.append(info)


    responseName = requests.get('https://api.github.com/repos/stlrda/redb_python/contents/python/DAGs')
    dagName = json.loads(responseName.text)

    counter = 0
    for data in dagName:
        info = {'name' : data['name']}
        DAGs[counter].update(info)
        counter += 1

    for dag in DAGs:
        
        #Convert current time to ISO 8601 format
        current_time = datetime.now().isoformat()
        timeNow = f"{current_time[:19]}Z"

        dagName = dag['name']
        dagAge = datetime.strptime(dag['age'], '%Y-%m-%dT%H:%M:%SZ')
        currentTime = datetime.strptime(timeNow, '%Y-%m-%dT%H:%M:%SZ')

        #Convert from Githubs UTC time to CST
        dagAge = dagAge - timedelta(hours=6)


        timeDifference = currentTime - dagAge
        sixHours = timedelta(hours=6)

        #If a DAG has been updated in the past 6 hours, then the updated version is downloaded.
        if timeDifference > sixHours:
            print (f'DAG({dagName}) is {timeDifference} old.')
        else:
            print("A newer version of the DAG has been found.")

            url = f"https://raw.githubusercontent.com/stlrda/redb_python/master/python/DAGs/{dagName}"
            r = requests.get(url)

            filename = f"C:/Users/abm0406/Desktop/test_directory/DAGs/{dagName}"
            with open(filename, 'wb') as f:
                f.write(r.content)

            print (f"DAG({dagName}) has been downloaded to {filename}")


if __name__ == "__main__":
    DAG_Updater()