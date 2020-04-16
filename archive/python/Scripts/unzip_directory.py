import os
import zipfile
import glob

working_directory = '/Users/jonathanleek/Desktop/working' # TODO modify unzip_directory to utilize s3 bucket

def unpack_dir(mypath):
    zipcounter = len(glob.glob1(mypath,"*.zip"))
    if zipcounter == 0:
        print("Done!")
    else:
        for item in os.listdir(mypath): #loop through items in dir
            if item.endswith('.zip'):  # check for ".zip" extension
                arch_name = mypath + '/' + item  # get full path of files
                print(arch_name)
                zip_ref = zipfile.ZipFile(arch_name)  # create zipfile object
                zip_ref.extractall(mypath + '/' + item[:-4])  # extract file to dir
                zip_ref.close()  # close file
                os.remove(arch_name)  # delete zipped file
                unpack_dir(mypath + '/' + item[:-4]) # TODO this only does 2 levels of unzip. make fully recursive?
        unpack_dir(mypath)

#unpack_dir('/Users/jonathanleek/Desktop/working')