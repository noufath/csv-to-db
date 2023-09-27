# D:/code/wsl/playground/csv2db

import os
import pandas as pd


# Assign variable location
sourceLoc = os.getcwd()+"/"
outLoc = os.getcwd()+"/result"
searchString = "lorem"
fileType = ".txt"

# list all the files in source location
direc=os.listdir(sourceLoc)

# create an empty list
fileList = []

# loop to search for the string
for file in direc:
    if file.endswith(fileType):
        f=open(sourceLoc+file,'r')
        if searchString in f.read():
            fileList.append(file)
        f.close()

# DataFrame creation and export to excel
stringFile=pd.DataFrame(fileList, columns=['FileName'], index=range(0,len(fileList)))
print(stringFile)

