from sqlalchemy import create_engine
import sys 
import os

sys.path.append(os.path.abspath("c:/Users/abm0406/Desktop"))
from Credentials import *

#Postgres username, password, and endpoint variables are all stored in a separate file
engine = create_engine(f'postgresql://{PGusername}:{PGpassword}@{PGendpoint}:5432/postgres')



tables = ['BldgCom', 'BldgRes', 'BldgResimp', 'BldgSect', 'Prcl', 'PrclAddrLRMS', 'PrclAsmt','PrclImp']
counter = 1

#Creates the Parcel11 from the Cityblock, Parcel, and Ownercode columns
def parcel_11(table_name):
    return (f"""REPLACE(CONCAT(TO_CHAR({table_name}."CityBlock"::float8,'FM0000.00'),TO_CHAR({table_name}."Parcel"::int8,'FM0000'),
    TO_CHAR({table_name}."OwnerCode"::int8,'FM0')),'.','') AS parcel_11_constr""")

with engine.connect() as con:

    for table in tables:

#Query to select the items not found in the opposing table and return their parcel11 code along with a note of which table they are missing from.
#My Postgres server was setup with two Databases: Staging 1 and Staging 2. The Staging 2 data was imported into Staging 1 via a foreign data wrapper under the schema "test".
        query = con.execute(f"""
        SELECT
        {parcel_11(table)},
        'not in Staging1' AS note
        FROM
            test.{table}
        EXCEPT
            SELECT
                {parcel_11(table)},
                'not in Staging1' AS note
            FROM
                public.{table}
        UNION
        SELECT
            {parcel_11(table)},
            'not in Staging2' AS note
        FROM
            public.{table}
        EXCEPT
            SELECT
                {parcel_11(table)},
                'not in Staging2' AS note
            FROM
                test.{table}

        """)

        print (f"Table Name: {table}  {counter}/{len(tables)}")
        print (f"----------------------------------")

        for row in query:
            print (f"Parcel11: {row[0]}  Notes: {row[1]}")
        print ("\n")
        counter += 1
