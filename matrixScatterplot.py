# Author: Wenqing Wu
# Date: 4/27/23
# Note: This will work with Layla's dataNormalization.py script for the orsfrs data

import numpy as np
import pandas as pd
import csv
import matplotlib.pyplot as plt # for data visualization
import plotly.express as px # for data visualization

# stripping unnecessary text from cells
text = open('matrix_result.csv', 'r')
text = ''.join([i for i in text]).replace("{'Failed': ", '')
text = ''.join([i for i in text]).replace("{'Unexpected': ", '')
text = ''.join([i for i in text]).replace("{'Unable': ", '')
text = ''.join([i for i in text]).replace('}', '')
x1 = open('matrix_result_output.csv', 'w')
x1.writelines(text)
x1.close()

# input file to be changed
df = pd.read_csv('matrix_result_output.csv')

x = df.columns[4:]  # defining the x-axis (the hours)

# defining y-axis (different error types)
y1 = df.loc[0][4:]
y2 = df.loc[1][4:]
y3 = df.loc[2][4:]

plt.figure(figsize=(10, 8))
# scatterplot of the values of Failed error counts for each hour
plt.scatter(x, y1, s=50, c='plum', label='Failed')
# scatterplot of the values of Unexpected error counts for each hour
plt.scatter(x, y2, s=50, c='darkcyan', label='Unexpected')
# scatterplot of the values of Unable error counts for each hour
plt.scatter(x, y3, s=50, c='greenyellow', label='Unable')
plt.legend(title='Error Type')
plt.xlabel('Hour')
plt.ylabel('Error Count')
plt.title('Error Type vs Error Count')
plt.show()
