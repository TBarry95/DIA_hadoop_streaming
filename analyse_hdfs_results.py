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

##########################################################
#. Extract: Read in files from HDFS:
##########################################################

hdfs_date_results = pd.read_csv("/home/tiernan/PycharmProjects/DIA/output_date_apr5.csv")
hdfs_acc_results = pd.read_csv("/home/tiernan/PycharmProjects/DIA/output_acc_apr5.csv")

##########################################################
#. Transform:
##########################################################

# -- Fix headings
hdfs_date_results.columns = ['DATE', 'SOURCE', 'MEAN_SENT', 'STND_DEV_SENT', 'MEDIAN_SENT',
       'MIN_SENT', 'MAX_SENT', 'FAVS_PER_TWEETS', 'RT_PER_TWEET', 'TWEETS_PER_DATE']

hdfs_acc_results.columns = ['SOURCE', 'MEAN_SENT', 'STND_DEV_SENT', 'MEDIAN_SENT', 'MIN_SENT',
       'MAX_SENT', 'MAX_FLWR', 'FAV_TO_FLWR', 'RT_TO_FLWR', 'TWEETS_PER_ACC']

##########################################################
#. Analysis 1: Dataset reduced by date:
#. 1. Plot number of tweets
#. 2. Based on above, plot summary statistics:
#. 3.
##########################################################
# -- Define plotting function:
def plot_x_last_x_days(data, stat, days, title, ylabel, pct_ch = None):
    hdfs_results_plot90 = data[len(data)-days:len(data)]
    # -- yy-mm
    #date_plot_format = [i[2:7] for i in hdfs_results_plot90['DATE']]
    plt.figure()
    if pct_ch == "YES":
        plt.plot([i for i in range(1,len(hdfs_results_plot90[stat])+1)], hdfs_results_plot90[stat].pct_change())
        plt.xlabel("Number of Days")
        plt.ylabel(ylabel)
        plt.title(title)
    else:
        plt.plot([i for i in range(1, len(hdfs_results_plot90[stat]) + 1)], hdfs_results_plot90[stat])
        plt.xlabel("Number of Days")
        plt.ylabel(ylabel)
        plt.title(title)
    return plt.show()

#. 1. Plot number of tweets by TWEETS_PER_DATE
plot_x_last_x_days(hdfs_date_results, "TWEETS_PER_DATE", len(hdfs_date_results), "Number of Tweets by Day", "Number of Tweets")

# -- Transform to percentage for secondary plot:
plot_x_last_x_days(hdfs_date_results, "TWEETS_PER_DATE", len(hdfs_date_results), "Number of Tweets by Day", "Number of Tweets", "YES")

#. 2. Based on above, plot summary statistics:
plot_x_last_x_days(hdfs_date_results, "MEAN_SENT", 90, "Mean sentiment 90 days", "Mean Sentiment")
plot_x_last_x_days(hdfs_date_results, "STND_DEV_SENT", 90, "Standard Deviation of Sentiment 90 days", "Standard Deviation")
plot_x_last_x_days(hdfs_date_results, "FAVS_PER_TWEETS", 90, "Mean Favourite per Tweet 90 days", "Mean Favourite per Tweet")
plot_x_last_x_days(hdfs_date_results, "RT_PER_TWEET", 90, "Mean RT per Tweet 90 days", "Mean RT")

##########################################################
#. Analysis 2: Dataset reduced by account:
#. 1.
#. 2.
#. 3.
##########################################################

#. 1. Plot number of tweets by TWEETS_PER_DATE
plot_x_last_x_days(hdfs_acc_results, "TWEETS_PER_ACC", len(hdfs_acc_results), "Number of Tweets per Account", "Number of Tweets")

# -- Transform to percentage for secondary plot:
plot_x_last_x_days(hdfs_date_results, "TWEETS_PER_DATE", len(hdfs_date_results), "Number of Tweets by Day", "Number of Tweets", "YES")

#. 2. Based on above, plot summary statistics:
plot_x_last_x_days(hdfs_date_results, "MEAN_SENT", 90, "Mean sentiment 90 days", "Mean Sentiment")
plot_x_last_x_days(hdfs_date_results, "STND_DEV_SENT", 90, "Standard Deviation of Sentiment 90 days", "Standard Deviation")
plot_x_last_x_days(hdfs_date_results, "FAVS_PER_TWEETS", 90, "Mean Favourite per Tweet 90 days", "Mean Favourite per Tweet")
plot_x_last_x_days(hdfs_date_results, "RT_PER_TWEET", 90, "Mean RT per Tweet 90 days", "Mean RT")
