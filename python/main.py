from python.download_city_file import download_city_file
from python.retrieve_city_file_array import files_to_download
from python.variables import entity_database_name, source_urls, lambda_folder


#Create function looping through, downloading files to ~

db_file_array = files_to_download(source_urls)

# for each array row in 'db_file_array'
for row in db_file_array:
    # download_city_file(row[2], lambda_folder + row[0])
    print(row)

    # download_city_file(row[3], lambda_folder)




#Unzip Files to ~ until no longer .zip



#determine file type and read

#export to ~ delimited file, save to S3

#Boto is library for AWS functions