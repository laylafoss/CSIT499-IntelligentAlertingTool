#Author: Layla Foss
#Date: 2/28/23
#Notes: Runs through data in ~2 minutes.
import pandas as pd
import numpy as np
import csv

#FUNCTIONS     

#Warning
def warningLog(df, warningData, rowIndex):        
    #taking index of string and pushing data to appropriate column
    splitWarning = warningData.split(' ')                #splitting data contained in warningData by space delimiter
    df.at[rowIndex, 'raw_date'] = splitWarning[0]        #datasheet at that row, pushing data from index 0 of warningData to raw_date column
    df.at[rowIndex, 'raw_time'] = splitWarning[1]        #datasheet at that row, pushing data from index 1 of warningData to raw_time column
    df.at[rowIndex, 'raw_PID'] = splitWarning[2]         #datasheet at that row, pushing data from index 2 of warningData to raw_PID column

    index = warningData.find('WARNING:')                 #finding keyword in row data, will never return -1 since we determine that in main script execution
    df.at[rowIndex, 'raw_message'] = warningData[index:] #inserts data from keyword to the end of the row

#ORSFRS
def orsfrsLog(df, orsfrsData, rowIndex):
    splitOrsfrs = orsfrsData.split(' ')
    df.at[rowIndex, 'raw_date'] = splitOrsfrs[0]
    df.at[rowIndex, 'raw_time'] = splitOrsfrs[1]
    df.at[rowIndex, 'raw_PID'] = splitOrsfrs[2]

    index = orsfrsData.find('ORSFRS')
    df.at[rowIndex, 'raw_message'] = orsfrsData[index:]

#Request
def requestLog(df, requestData, rowIndex):
    splitOrsfrs = requestData.split(' ')
    df.at[rowIndex, 'raw_date'] = splitOrsfrs[0]
    df.at[rowIndex, 'raw_time'] = splitOrsfrs[1]
    df.at[rowIndex, 'raw_PID'] = splitOrsfrs[2]

    index = requestData.find('Request')
    df.at[rowIndex, 'raw_message'] = requestData[index:]

#Requesting
def requestingLog(df, requestingData, rowIndex):
    splitOrsfrs = requestingData.split(' ')
    df.at[rowIndex, 'raw_date'] = splitOrsfrs[0]
    df.at[rowIndex, 'raw_time'] = splitOrsfrs[1]
    df.at[rowIndex, 'raw_PID'] = splitOrsfrs[2]

    index = requestingData.find('Requesting')
    df.at[rowIndex, 'raw_message'] = requestingData[index:]

#Report
def reportLog(df, reportData, rowIndex):
    splitOrsfrs = reportData.split(' ')
    df.at[rowIndex, 'raw_date'] = splitOrsfrs[0]
    df.at[rowIndex, 'raw_time'] = splitOrsfrs[1]
    df.at[rowIndex, 'raw_PID'] = splitOrsfrs[2]

    index = reportData.find('Report')
    df.at[rowIndex, 'raw_message'] = reportData[index:]

#No valid PDF
def novalidpdfLog(df, novalidpdData, rowIndex):
    splitOrsfrs = novalidpdData.split(' ')
    df.at[rowIndex, 'raw_date'] = splitOrsfrs[0]
    df.at[rowIndex, 'raw_time'] = splitOrsfrs[1]
    df.at[rowIndex, 'raw_PID'] = splitOrsfrs[2]

    index = novalidpdData.find('No valid')
    df.at[rowIndex, 'raw_message'] = novalidpdData[index:]

#Unable
def unableLog(df, unableData, rowIndex):
    splitOrsfrs = unableData.split(' ')
    df.at[rowIndex, 'raw_date'] = splitOrsfrs[0]
    df.at[rowIndex, 'raw_time'] = splitOrsfrs[1]
    df.at[rowIndex, 'raw_PID'] = splitOrsfrs[2]

    index = unableData.find('Unable')
    df.at[rowIndex, 'raw_message'] = unableData[index:]

#Shortest
def shortestLog(df, shortestData, rowIndex):
    splitOrsfrs = shortestData.split(' ')
    df.at[rowIndex, 'raw_date'] = splitOrsfrs[0]
    df.at[rowIndex, 'raw_time'] = splitOrsfrs[1]
    df.at[rowIndex, 'raw_PID'] = splitOrsfrs[2]

    index = shortestData.find('Shortest')
    df.at[rowIndex, 'raw_message'] = shortestData[index:]

#ORA
def oraLog(df, oraData, rowIndex):
    splitOrsfrs = oraData.split(' ')
    df.at[rowIndex, 'raw_date'] = splitOrsfrs[0]
    df.at[rowIndex, 'raw_time'] = splitOrsfrs[1]
    df.at[rowIndex, 'raw_PID'] = splitOrsfrs[2]

    index = oraData.find('ORA-')
    df.at[rowIndex, 'raw_message'] = oraData[index:]

#Error
def errorLog(df, errorData, rowIndex):
    splitOrsfrs = errorData.split(' ')
    df.at[rowIndex, 'raw_date'] = splitOrsfrs[0]
    df.at[rowIndex, 'raw_time'] = splitOrsfrs[1]
    df.at[rowIndex, 'raw_PID'] = splitOrsfrs[2]

    index = errorData.find('Error')
    df.at[rowIndex, 'raw_message'] = errorData[index:]

#Unexpected
def unexpectedLog(df, unexpectedData, rowIndex):
    splitOrsfrs = unexpectedData.split(' ')
    df.at[rowIndex, 'raw_date'] = splitOrsfrs[0]
    df.at[rowIndex, 'raw_time'] = splitOrsfrs[1]
    df.at[rowIndex, 'raw_PID'] = splitOrsfrs[2]

    index = unexpectedData.find('Unexpected')
    df.at[rowIndex, 'raw_message'] = unexpectedData[index:]

#Prepare
def prepareLog(df, prepareData, rowIndex):
    splitOrsfrs = prepareData.split(' ')
    df.at[rowIndex, 'raw_date'] = splitOrsfrs[0]
    df.at[rowIndex, 'raw_time'] = splitOrsfrs[1]
    df.at[rowIndex, 'raw_PID'] = splitOrsfrs[2]

    index = prepareData.find('Prepare')
    df.at[rowIndex, 'raw_message'] = prepareData[index:]

#Database
def databaseLog(df, databaseData, rowIndex):
    splitOrsfrs = databaseData.split(' ')
    df.at[rowIndex, 'raw_date'] = splitOrsfrs[0]
    df.at[rowIndex, 'raw_time'] = splitOrsfrs[1]
    df.at[rowIndex, 'raw_PID'] = splitOrsfrs[2]

    index = databaseData.find('Database')
    df.at[rowIndex, 'raw_message'] = databaseData[index:]

#Retrying
def retryingLog(df, retryingData, rowIndex):
    splitOrsfrs = retryingData.split(' ')
    df.at[rowIndex, 'raw_date'] = splitOrsfrs[0]
    df.at[rowIndex, 'raw_time'] = splitOrsfrs[1]
    df.at[rowIndex, 'raw_PID'] = splitOrsfrs[2]

    index = retryingData.find('Retrying')
    df.at[rowIndex, 'raw_message'] = retryingData[index:]

#Session
def sessionLog(df, sessionData, rowIndex):
    splitOrsfrs = sessionData.split(' ')
    df.at[rowIndex, 'raw_date'] = splitOrsfrs[0]
    df.at[rowIndex, 'raw_time'] = splitOrsfrs[1]
    #no PID included in Session logs

    index = sessionData.find('Session')
    df.at[rowIndex, 'raw_message'] = sessionData[index:]

#For any data not accounted for in script or unexpected inserts
def handleUnexpected(df, rowIndex):
    df.at[rowIndex, 'is_error'] = 1          #can be changed


#MAIN EXECUTION   
 
df = pd.read_csv('ORS_Data_Updated.csv')     #reading csv file
df = df.applymap(str)                        #all entries in DF converted to str

#Create columns: does not overwrite existing columns
columnLen = len(df.columns)                  #finding length of columns, adds new columns at end of sheet
df.insert(columnLen, "raw_date", '')
df.insert(columnLen + 1, 'raw_time', '')
df.insert(columnLen + 2, 'raw_PID', '')
df.insert(columnLen + 3, 'raw_message', '')
df.insert(columnLen + 4, 'is_error', 0)

#iterate over every row
row = 0
rawArr = df["_raw"].to_numpy()               #convert column to array to iterate over
for data in rawArr:
    print("Processing row: ", row)           #displays row processing
                                             #SAME FOR ALL FUNCTIONS IN FOR LOOP:
    if(data.find('WARNING:') != -1):         #finding keyword for row
        warningLog(df, data, row)            #executes function based on row keyword
    elif(data.find('ORSFRS') != -1):
        orsfrsLog(df, data, row)
    elif(data.find('Request') != -1):
        requestLog(df, data, row)
    elif(data.find('Requesting') != -1):
        requestingLog(df, data, row)
    elif(data.find('Report') != -1):
        reportLog(df, data, row)
    elif(data.find('No valid') != -1):
        novalidpdfLog(df, data, row)
    elif(data.find('Unable') != -1):
        unableLog(df, data, row)
    elif(data.find('Shortest') != -1):
        shortestLog(df, data, row)
    elif(data.find('ORA-') != -1):
        oraLog(df, data, row)
    elif(data.find('Error') != -1):
        errorLog(df, data, row)
    elif(data.find('Unexpected') != -1):
        unexpectedLog(df, data, row)
    elif(data.find('Prepare') != -1):
        prepareLog(df, data, row)
    elif(data.find('Database') != -1):
        databaseLog(df, data, row)
    elif(data.find('Retrying') != -1):
        retryingLog(df, data, row)
    elif(data.find('Session') != -1):
        sessionLog(df, data, row)
    else:
        handleUnexpected(df, row)

    print("Finished row: ", row)           #concludes row being processed
    row += 1

df.to_csv('output.csv')                    #output file, needs to be changed