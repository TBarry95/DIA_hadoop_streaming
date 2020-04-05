import numpy as np
import pandas as pd
import re
# import missingno as msno
import matplotlib.pyplot as plt
import warnings
import json
import functions_tweet_mapreduce as fns
import csv
import datetime

hdfs_results = pd.read_csv("/home/tiernan/PycharmProjects/DIA/output_date_sentiment")
hdfs_results.columns = ['DATE', 'SOURCE', 'MEAN', 'STND_DEV', 'MEDIAN', 'MIN', 'MAX','COUNT']

# Mean:
# plot last 90 days:
hdfs_results_plot90 = hdfs_results[len(hdfs_results)-40:len(hdfs_results)]
# yy-mm
#date_plot_format = [i[2:7] for i in hdfs_results_plot90['DATE']]

plt.figure()
plt.plot([i for i in range(1,len(hdfs_results_plot90['MEAN'])+1)], hdfs_results_plot90['MEAN'])
plt.xlabel("Number of Days")
plt.ylabel("Average Daily Sentiment")
plt.title("X days - Average Daily Sentiment")

# Median:
hdfs_results_plot90 = hdfs_results[len(hdfs_results)-40:len(hdfs_results)]
# yy-mm
#date_plot_format = [i[2:7] for i in hdfs_results_plot90['DATE']]

plt.figure()
plt.plot([i for i in range(1,len(hdfs_results_plot90['MEAN'])+1)], hdfs_results_plot90['MEDIAN'])
plt.xlabel("Number of Days")
plt.ylabel("MEDIAN Daily Sentiment")
plt.title("X days - MEDIAN Daily Sentiment")

# standard deviation:
hdfs_results_plot90 = hdfs_results[len(hdfs_results)-200:len(hdfs_results)]
# yy-mm
#date_plot_format = [i[2:7] for i in hdfs_results_plot90['DATE']]

plt.figure()
plt.plot([i for i in range(1,len(hdfs_results_plot90['MEAN'])+1)], hdfs_results_plot90['STND_DEV'])
plt.xlabel("Number of Days")
plt.ylabel("STND_DEV Daily Sentiment")
plt.title("X days - STND_DEV Daily Sentiment")





