import csv
from boto3 import client
import boto3
import os
import subprocess
from io import StringIO
import argparse
from sqlalchemy import create_engine
import pandas as pd


parser = argparse.ArgumentParser()
parser.add_argument("--bucket", "-b", help ='set S3 bucket name')
parser.add_argument("--profile", "-p", help ='set aws profile credentials')

args = parser.parse_args()



def psql_insert_copy(table, conn, keys, data_iter):
    # gets a DBAPI connection that can provide a cursor
    dbapi_conn = conn.connection
    with dbapi_conn.cursor() as cur:
        s_buf = StringIO()
        writer = csv.writer(s_buf)
        writer.writerows(data_iter)
        s_buf.seek(0)

        columns = ', '.join('"{}"'.format(k) for k in keys)
        if table.schema:
            table_name = '{}.{}'.format(table.schema, table.name)
        else:
            table_name = table.name

        sql = 'COPY {} ({}) FROM STDIN WITH CSV'.format(
            table_name, columns)
        cur.copy_expert(sql=sql, file=s_buf)



def table_data(table_names):
    PGusername = 'YourUsernameHere'
    PGpassword = 'YourPasswordHere'
    PGendpoint = 'YourEndpointHere'

    cwd = os.getcwd()
    print(cwd)
    path = (f'{cwd}/csv')
    try:
        os.mkdir(path)
    except:
        pass
    
    for key, listValue in table_names.items():
        for item in listValue:
            print(f'Working on table {item} in database {key}')
            tableData_byte = subprocess.Popen(["mdb-export", f'/home/ubuntu/Inputs/{key}', f'{item}'], stdout=subprocess.PIPE).communicate()[0]
            s=str(tableData_byte,'utf-8')
            data = StringIO(s)
            df=pd.read_csv(data, low_memory=False)

    engine = create_engine(f'postgresql://{PGusername}:{PGpassword}@{PGendpoint}:5432/postgres')
    
    df.to_sql(table_name.lower(), engine, if_exists='replace', method=psql_insert_copy)
    print(f'{table_name} inserted into PG')




def get_tables(path):
    table_names = {}
    for file in os.listdir(path):
        table_byte = subprocess.Popen(["mdb-tables", "-1", f'{path}/{file}'], stdout=subprocess.PIPE).communicate()[0]
        names = []
        for x in table_byte.split(b"\n"):
            names.append(x.decode('utf-8'))
        names.pop()
        table_names[file] = names
    
    table_data(table_names)



def s3_download(bucket, mdbList):
    counter = 1
    cwd = os.getcwd()
    path = (f'{cwd}/Inputs')
    try:
        os.mkdir(path)
    except:
        pass
    
    for key in mdbList:
        s3 = boto3.resource('s3')
        s3.Bucket(bucket).download_file(key, f'{path}/my_local_{key.rpartition("/")[2]}')
        print (f"Downloaded {key} {counter}/{len(mdbList)} ")
        counter += 1

    get_tables(path)


def filter_mdb(bucket, profile='default'):
    boto3.setup_default_session(profile_name=profile)
    conn = client('s3')
    mdbList = []
    for key in conn.list_objects(Bucket=bucket)['Contents']:
        if key['Key'][-4:] == ".mdb":
            mdbList.append(key['Key'])
        print(key['Key'])
    
    s3_download(bucket, mdbList)  


filter_mdb(args.bucket,args.profile)


