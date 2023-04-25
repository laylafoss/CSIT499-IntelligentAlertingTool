#Author: Layla Foss
#Date: 4/16/23
import pandas as pd
import numpy as np
import csv

#        MAIN EXECUTION    
df = pd.read_csv('ORS_Updated_Data2.csv')            #reading csv file
df = df.applymap(str)                        #all entries in DF converted to str

#iterate over every row
row = 0
failed_count = {'Failed':0}
unexpected_count = {'Unexpected':0}
unable_count = {'Unable':0}

rawArr = df["_raw"].to_numpy()               #convert column to array to iterate over
for data in rawArr:
    print("Processing row: ", row)           #displays row processing
                                             #SAME FOR ALL FUNCTIONS IN FOR LOOP:
    if(data.find('Failed') != -1):           #finding keyword for row
        sourceName = df.at[row, 'source']
        hostName = df.at[row, 'host']
        failed_count['Failed'] = failed_count['Failed'] + 1
    elif(data.find('Unexpected') != -1):
        hostName = df.at[row, 'host']
        unexpected_count['Unexpected'] = unexpected_count['Unexpected'] + 1
    elif(data.find('Unable') != -1):
        hostName = df.at[row, 'host']
        unable_count['Unable'] = unable_count['Unable'] + 1

    # Creating dataframe
    failedData = [[sourceName, hostName, 'Failed', failed_count['Failed']]]
    unexpectedData = [[sourceName, hostName, 'Unexpected', unexpected_count['Unexpected']]]
    unableData = [[sourceName, hostName, 'Unable', unable_count['Unable']]]

    df1=pd.DataFrame(failedData, columns=['Source', 'Host', 'Error Type', 'Count'])
    df2=pd.DataFrame(unexpectedData, columns=['Source', 'Host', 'Error Type', 'Count'])
    df3=pd.DataFrame(unableData, columns=['Source', 'Host', 'Error Type', 'Count'])
    print("Finished row: ", row)           
    row += 1

result = pd.concat([df1, df2, df3])
result.to_csv('matrix_result.csv')