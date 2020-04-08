#!/usr/bin/python

# SCRIPT 7: Get data from Ubuntu EC2 instance (data was manually copied from HDS to Ubuntu EC2).

# DES:
# BY:  Tiernan Barry, x19141840 - NCI.

# Libraries:
import numpy as np
import pandas as pd
import re
import matplotlib.pyplot as plt
import warnings
import json
import functions_tweet_mapreduce as fns
import csv
import datetime

# Installations (if needed):
# pip install pysftp
# import missingno as msno

##########################################################
#. Transform:
##########################################################

# -- Fix headings
hdfs_date_results.columns = ['DATE', 'SOURCE', 'MEAN_SENT', 'STND_DEV_SENT', 'MEDIAN_SENT',
       'MIN_SENT', 'MAX_SENT', 'FAVS_PER_TWEETS', 'RT_PER_TWEET', 'TWEETS_PER_DATE']

hdfs_acc_results.columns = ['SOURCE', 'MEAN_SENT', 'STND_DEV_SENT', 'MEDIAN_SENT', 'MIN_SENT',
       'MAX_SENT', 'MAX_FLWR', 'FAV_TO_FLWR', 'RT_TO_FLWR', 'TWEETS_PER_ACC']

# -- Ensure no duplicates:
hdfs_date_results.drop_duplicates()
hdfs_acc_results.drop_duplicates()

# -- Transform names into abbreviated names for easier graphing hdfs_acc_results:
'''for i in hdfs_acc_results['SOURCE']:
    if len(i) > 6:
        print(i)'''

##########################################################
#. Analysis 1: Dataset reduced by date:
#. 1. Plot number of tweets by date.
#. 2. Based on above, plot summary statistics by date.
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
plot_x_last_x_days(hdfs_date_results, "TWEETS_PER_DATE", len(hdfs_date_results), "% Change Number of Tweets by Day", "% Change", "YES")

#. 2. Based on above, plot summary statistics:
plot_x_last_x_days(hdfs_date_results, "MEAN_SENT", 90, "Mean sentiment 90 days", "Mean Sentiment")
plot_x_last_x_days(hdfs_date_results, "STND_DEV_SENT", 90, "Standard Deviation of Sentiment 90 days", "Standard Deviation")
plot_x_last_x_days(hdfs_date_results, "FAVS_PER_TWEETS", 90, "Mean Favourite per Tweet 90 days", "Mean Favourite per Tweet")
plot_x_last_x_days(hdfs_date_results, "RT_PER_TWEET", 90, "Mean RT per Tweet 90 days", "Mean RT")

##########################################################
#. Analysis 2: Dataset reduced by account:
#. 1. Tweets by account = 1800
#. 2. Plotting average sentiment
#. 3. Get top 10 and bottom 10
##########################################################
# -- Define plotting function:
def plot_x_last_x_days_acc(data, stat, x_axis_label, days, title, ylabel, pct_ch = None):
    hdfs_results_plot90 = data[len(data)-days:len(data)]
    # -- yy-mm
    #date_plot_format = [i[2:7] for i in hdfs_results_plot90['DATE']]
    plt.figure()
    if pct_ch == "YES":
        plt.plot([i for i in x_axis_label], hdfs_results_plot90[stat].pct_change())
        plt.xlabel("List of Accounts")
        plt.ylabel(ylabel)
        plt.title(title)
    else:
        plt.plot([i for i in x_axis_label], hdfs_results_plot90[stat])
        plt.xlabel("List of Accounts")
        plt.ylabel(ylabel)
        plt.title(title)
    return plt.show()

#. 1. Plotting average sentiment
plot_x_last_x_days_acc(hdfs_acc_results, 'MEAN_SENT', [i for i in range(0,len(hdfs_acc_results))], len(hdfs_acc_results), "Mean Sentiment per Account", "Mean Sentiment")

#. 2. Sort by sentiment:
hdfs_acc_results = hdfs_acc_results.sort_values(by='MEAN_SENT')
bottom_x_sentiment = hdfs_acc_results[0:5]
top_x_sentiment = hdfs_acc_results[len(hdfs_acc_results)-5:]

#. 2. Plot top 10 v Bottom 10:
plot_x_last_x_days_acc(top_x_sentiment, 'MEAN_SENT', [i for i in top_x_sentiment['SOURCE']], len(top_x_sentiment), "Top 5 by sentiment", "Mean Sentiment")
plot_x_last_x_days_acc(bottom_x_sentiment, 'MEAN_SENT', [i for i in bottom_x_sentiment['SOURCE']], len(bottom_x_sentiment), "Bottom 5 by sentiment", "Mean Sentiment")


top_10_sentiment[['SOURCE', 'MEAN_SENT']].plot(kind = 'bar', )



