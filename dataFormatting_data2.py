#Author: Layla Foss
#Date: 2/28/23
#Notes: Runs through data in ~2 minutes.
import pandas as pd
import numpy as np
import re
import csv

#        FUNCTIONS     

#Failed
def failedLog(df, failedData, rowIndex):        
    #taking index of string and pushing data to appropriate column
    #re allows for multiple types of delimiters, separated by the OR operator
    splitFailed = re.split(' |,', failedData)           #splitting data contained in failedData by space and comma delimiter 
    df.at[rowIndex, 'raw_date'] = splitFailed[0]        #datasheet at that row, pushing data from index 0 of failedData to raw_date column
    df.at[rowIndex, 'raw_time'] = splitFailed[1]        #datasheet at that row, pushing data from index 1 of failedData to raw_time column
    df.at[rowIndex, 'raw_ms'] = splitFailed[2]          #datasheet at that row, pushing data from index 2 of failedData to raw_ms column
    df.at[rowIndex, 'raw_PID'] = splitFailed[3]         #datasheet at that row, pushing data from index 3 of failedData to raw_PID column
    df.at[rowIndex, 'raw_message'] = splitFailed[4]     #datasheet at that row, pushing data from index 4 of failedData to raw_message column

    index = failedData.find('Failed')                    #finding keyword in row data, will never return -1 since we determine that in main script execution
    df.at[rowIndex, 'raw_message'] = failedData[index:]  #inserts data from keyword to the end of the row

#Unexpected
def unexpectedLog(df, unexpectedData, rowIndex):       
    splitUnexpected = re.split(' |,', unexpectedData)       
    df.at[rowIndex, 'raw_date'] = splitUnexpected[0]        
    df.at[rowIndex, 'raw_time'] = splitUnexpected[1]        
    df.at[rowIndex, 'raw_ms'] = splitUnexpected[2]         
    df.at[rowIndex, 'raw_PID'] = splitUnexpected[3]         
    df.at[rowIndex, 'raw_message'] = splitUnexpected[4]     

    index = unexpectedData.find('Unexpected')                 
    df.at[rowIndex, 'raw_message'] = unexpectedData[index:]  

#Unable
def unableLog(df, unableData, rowIndex):
    splitUnable = re.split(' |,', unableData)       
    df.at[rowIndex, 'raw_date'] = splitUnable[0]        
    df.at[rowIndex, 'raw_time'] = splitUnable[1]        
    df.at[rowIndex, 'raw_ms'] = splitUnable[2]         
    df.at[rowIndex, 'raw_PID'] = splitUnable[3]         
    df.at[rowIndex, 'raw_message'] = splitUnable[4]         

    index = unableData.find('Unable')                 
    df.at[rowIndex, 'raw_message'] = unableData[index:] 

#For any data not accounted for in script or unexpected inserts
def handleUnexpected(df, rowIndex):
    df.at[rowIndex, 'is_error'] = 1           #can be changed


#          MAIN EXECUTION    
df = pd.read_csv('test_data_2.csv')           #reading csv file
df = df.applymap(str)                         #all entries in DF converted to str

#Create columns: does not overwrite existing columns
columnLen = len(df.columns)                  #finding length of columns, adds new columns at end of sheet
df.insert(columnLen, "raw_date", '')
df.insert(columnLen + 1, 'raw_time', '')
df.insert(columnLen + 2, 'raw_ms', '')
df.insert(columnLen + 3, 'raw_PID', '')
df.insert(columnLen + 4, 'raw_message', '')
df.insert(columnLen + 5, 'is_error', 0)

#iterate over every row
row = 0
rawArr = df["_raw"].to_numpy()               #convert column to array to iterate over
for data in rawArr:
    print("Processing row: ", row)           #displays row processing
                                             #SAME FOR ALL FUNCTIONS IN FOR LOOP:
    if(data.find('Failed') != -1):           #finding keyword for row
        failedLog(df, data, row)             #executes function based on row keyword
    elif(data.find('Unexpected') != -1):
        unexpectedLog(df, data, row)
    elif(data.find('Unable') != -1):
        unableLog(df, data, row)
    else:
        handleUnexpected(df, row)

    print("Finished row: ", row)           #concludes row being processed
    row += 1

df.to_csv('output_2.csv')                    #output file, needs to be changed for DataBricks