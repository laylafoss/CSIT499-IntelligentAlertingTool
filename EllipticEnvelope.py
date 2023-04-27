# Elliptic Envelope machine learning algorithm
# Madeline Barlow 
# Contributed to by Wen Qing Wu

import numpy as np
import pandas as pd
import csv
import matplotlib.pyplot as plt # for data visualization
import plotly.express as px # for data visualization

from sklearn.covariance import EllipticEnvelope
from sklearn.datasets import make_blobs
from sklearn.preprocessing import LabelEncoder
from numpy import quantile, where, random
import matplotlib.pyplot as plt

df = pd.read_csv('matrix_result_copy.csv')      

# Extract digits from each cell
# Eventually to work this algorithm with our data from Layla's matrix, we will need to be able to
# extract the numbers from each cell to fit them into our envelope. 
# The script in its current state only works with the test datasheet we manufactured 
# for our purposes, not with the result Layla has come up with.

dropping = df.drop(df.columns[[0, 1, 2, 3]], axis=1) 

failed = dropping.iloc[0]
unexpected = dropping.iloc[1]
unable = dropping.iloc[2]

# Create an Elliptic Envelope object and fit it to the data
envelope = EllipticEnvelope(contamination=0.2)
envelope.fit(failed.values.reshape(-1,1))

# Use the model to predict the outlier status of each data point in new dataset
failed_pred = envelope.predict(failed.values.reshape(-1,1))
unexpected_pred = envelope.predict(unexpected.values.reshape(-1,1))
unable_pred = envelope.predict(unable.values.reshape(-1,1))

print('Failed error outliers: ', failed_pred)
print('Unexpected error outliers: ', unexpected_pred)
print('Unable error outliers: ', unable_pred)


# Print scatter plot to represent data 
# Author: Wen Qing Wu

x = df.columns[4:]

y1 = df.loc[0][4:]
y2 = df.loc[1][4:]
y3 = df.loc[2][4:]

plt.figure(figsize=(10, 8))
plt.scatter(x, y1, s=50, c='plum', label='Failed')
plt.scatter(x, y2, s=50, c='darkcyan', label='Unexpected')
plt.scatter(x, y3, s=50, c='greenyellow', label='Unable')
plt.legend(title='Error Type')
plt.xlabel('Hour')
plt.ylabel('Error Count')
plt.title('Error Type vs Error Count')
plt.show()
