#Author: Layla Foss
#Date: 4/16/23
#        FOR ORSFRS DATA
import pandas as pd
import numpy as np
import csv

#        MAIN EXECUTION    
df = pd.read_csv('ORSUpdatedData2.csv')        #reading csv file
df = df.applymap(str)                          #all entries in DF converted to str

row = 0

#setting counts to 0
failed_count = {'Failed':0}             
unexpected_count = {'Unexpected':0}
unable_count = {'Unable':0}

#setting initial time to first record, this will always be the first row hour
current_hour = df.at[row, 'date_hour']

#only works for one source and host, more complex looping required to pull source and host from each row of data
sourceName = df.at[row, 'source']
hostName = df.at[row, 'host']

#populating data for dataframe
failedData = [[sourceName, hostName, 'Failed']]
unexpectedData = [[sourceName, hostName, 'Unexpected']]
unableData = [[sourceName, hostName, 'Unable']]

#creating dataframes, done in separate dataframes for each error type. One dataframe will not take more than one set of data
df1=pd.DataFrame(failedData, columns=['Source', 'Host', 'Error Type'])
df2=pd.DataFrame(unexpectedData, columns=['Source', 'Host', 'Error Type'])
df3=pd.DataFrame(unableData, columns=['Source', 'Host', 'Error Type'])

#iterate over every row
rawArr = df["_raw"].to_numpy()            #setting dataframe rows to numpy array
for data in rawArr:
    print("Processing row: ", row)

    #checking current hour for changes
    if(current_hour != df.at[row, 'date_hour']):
        print('Hour changed')
        columnLen = len(df1.columns)
        #inserting column named as the current hour, dumping failed count value. Allows for duplicates in data for day changes
        df1.insert(columnLen, current_hour, [failed_count], allow_duplicates=True)
        df2.insert(columnLen, current_hour, [unexpected_count], allow_duplicates=True)
        df3.insert(columnLen, current_hour, [unable_count], allow_duplicates=True)

        #changing current hour to the hour that it has been changed to
        current_hour = df.at[row, 'date_hour']

        #resetting counts to 0
        failed_count = {'Failed':0}
        unexpected_count = {'Unexpected':0}
        unable_count = {'Unable':0}

    #searching for each error type
    if(data.find('Failed') != -1):
        failed_count['Failed'] = failed_count['Failed'] + 1   #adds to count when found
    elif(data.find('Unexpected') != -1):
        unexpected_count['Unexpected'] = unexpected_count['Unexpected'] + 1
    elif(data.find('Unable') != -1):
        unable_count['Unable'] = unable_count['Unable'] + 1
    
    print("Finished row: ", row)           
    row += 1

print(failed_count)                                           #printing last count that occurs
result = pd.concat([df1, df2, df3], ignore_index=True)        #concatinating datatables together
result.to_csv('matrix_result.csv')