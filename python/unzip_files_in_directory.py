import os
import zipfile
import glob


def unpack_dir(mypath):
    zipcounter = len(glob.glob1(mypath,"*.zip"))
    if zipcounter == 0:
        print("DONE!")
    else:
        for item in os.listdir(mypath):  # loop through items in dir
            if item.endswith('.zip'):  # check for ".zip" extension
                arch_name = mypath + '/' + item  # get full path of files
                print(arch_name)
                zip_ref = zipfile.ZipFile(arch_name)  # create zipfile object
                zip_ref.extractall(mypath)  # extract file to dir
                zip_ref.close()  # close file
                os.remove(arch_name)  # delete zipped file
        unpack_dir(mypath)


