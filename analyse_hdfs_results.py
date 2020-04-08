#!/usr/bin/python

# SCRIPT 8: Analyse HDFS output, provide visualisations and insights.

# DES: Having completed 2 sentiment analyses using hadoop, this script
#      reads in the results of the analysis to highlight insights, and to visualise results.
# BY:  Tiernan Barry, x19141840 - NCI.

# Libraries:
import numpy as np
import pandas as pd
import re
import warnings
import json
import functions_tweet_mapreduce as fns
import csv
import datetime
import matplotlib.pyplot as plt
from bokeh.models import LinearAxis, Range1d
from bokeh.plotting import figure, output_file, ColumnDataSource, show
from bokeh.layouts import column, gridplot


# Installations (if needed):
# import missingno as msno

##########################################################################
# Extract: Read in data locally (after being called from HDFS in script 7)
##########################################################################

hdfs_date_results = pd.read_csv("/home/tiernan/PycharmProjects/DIA/date_apr8.csv")
hdfs_acc_results = pd.read_csv("/home/tiernan/PycharmProjects/DIA/account_apr8.csv")

##########################################################################
#. Transform:
##########################################################################

# -- Fix headings
hdfs_date_results.columns = ['DATE', 'SOURCE', 'MEAN_SENT', 'STND_DEV_SENT', 'MEDIAN_SENT',
       'MIN_SENT', 'MAX_SENT', 'FAVS_PER_TWEETS', 'RT_PER_TWEET', 'CORR_FAV_SENT', 'CORR_RT_SENT','TWEETS_PER_DATE']

hdfs_acc_results.columns = ['SOURCE', 'MEAN_SENT', 'STND_DEV_SENT', 'MEDIAN_SENT', 'MIN_SENT',
       'MAX_SENT', 'MAX_FLWR', 'FAV_TO_FLWR', 'RT_TO_FLWR', 'CORR_FAV_SENT', 'CORR_RT_SENT', 'TWEETS_PER_ACC']

# -- Ensure no duplicates:
hdfs_date_results.drop_duplicates()
hdfs_acc_results.drop_duplicates()

# -- Transform names into abbreviated names for easier graphing hdfs_acc_results:
'''for i in hdfs_acc_results['SOURCE']:
    if len(i) > 6:
        print(i)'''

##########################################################
#. Analysis 1: Dataset reduced by date: date_apr8.csv
#. 1. Plot number of tweets by date.
#. 2. Based on above, plot summary statistics by date.
#. 3. Correlations?
##########################################################

plt.figure()
plt.plot([i for i in range(0,60)], hdfs_date_results["FAVS_PER_TWEETS"][len(hdfs_date_results)-60:])
plt.plot([i for i in range(0,60)], hdfs_date_results["RT_PER_TWEET"][len(hdfs_date_results)-60:])
plt.xlabel("Number of Days")
plt.ylabel()
plt.title()
plt.legend()
# bokeh:

fav = hdfs_date_results["FAVS_PER_TWEETS"][len(hdfs_date_results)-60:]
rt = hdfs_date_results["RT_PER_TWEET"][len(hdfs_date_results)-60:]
days = [i for i in range(0,60)]
fav_rt = figure(x_axis_type="datetime", title="Fav to Follower vs RT to Follower Ratio", plot_height=600,plot_width=800,
                x_axis_label = "Days", y_axis_label = "Ratio")
fav_rt.extra_y_ranges= {"RT": Range1d(start=0, end=max(rt))}
fav_rt.add_layout(LinearAxis(y_range_name="RT"), "right")
fav_rt.line(days, fav, legend="Fav:Follower",alpha=0.8, color="#53777a")
fav_rt.line(days, rt, legend="RT:Follower", alpha=0.8, color="#c02942", y_range_name = "RT")
fav_rt.legend.location = "top_left"
fav_rt.legend.click_policy="hide"
show(fav_rt)

'''
px_s = [x for x in df_spx["Close Price"]]
dt_s = [dt.strptime(i, "%Y-%m-%d").date() for i in df_spx["Date"]]
px_p = [x for x in df_psa["Close Price"]]
dt_p = [dt.strptime(i, "%Y-%m-%d").date() for i in df_psa["Date"]]
px_a = [x for x in df_adbe["Close Price"]]
dt_a = [dt.strptime(i, "%Y-%m-%d").date() for i in df_adbe["Date"]]
px_h = [x for x in df_hngr["Close Price"]]
dt_h = [dt.strptime(i, "%Y-%m-%d").date() for i in df_hngr["Date"]]

# create 2x2 plots
s1s = figure(x_axis_type="datetime", title="SPX & PSA Hist. Return - Click Legend to Hide Stock", plot_height=600,plot_width=800,
                x_axis_label = "Time (years)", y_axis_label = "Price (USD)")
s1s.extra_y_ranges= {"Price2": Range1d(start=0, end=max(px_p))}
s1s.add_layout(LinearAxis(y_range_name="Price2"), "right")
s1s.line(dt_s, px_s, legend="SPX",alpha=0.8, color="#53777a")
s1s.line(dt_p, px_p, legend="PSA", alpha=0.8, color="#c02942", y_range_name = "Price2")
s1s.legend.location = "top_left"
s1s.legend.click_policy="hide"
s2s = figure(x_axis_type="datetime", title="ADBE & HNGR Hist. Return - Click Legend to Hide Stock", plot_height=600,plot_width=800,
                x_axis_label = "Time (years)", y_axis_label = "Price (USD)")
s2s.extra_y_ranges= {"Price2": Range1d(start=0, end=max(px_p))}
s2s.add_layout(LinearAxis(y_range_name="Price2"), "right")
s2s.line(dt_a, px_a, legend="ADBE",alpha=0.8, color="#53777a")
s2s.line(dt_h, px_h, legend="HNGR",alpha=0.8, color="#c02942", y_range_name = "Price2")
s2s.legend.location = "top_left"
s2s.legend.click_policy="hide"
grid_2 = gridplot([s1s,s2s], ncols=2, plot_width=400, plot_height=300)

show(grid_2)'''



#. 1. Plot number of tweets by TWEETS_PER_DATE
fns.plot_x_last_x_days(hdfs_date_results, "TWEETS_PER_DATE", len(hdfs_date_results), "Number of Tweets by Day", "Number of Tweets")
# -- Total days = 1337, using 30-40 days means theres around 600+ tweets per day.
# -- Total days = 1337, using 70 days means theres around 200+ tweets per day.
# -- Total days = 1337, using 100 days means theres around 50-100+ tweets per day.
# -- Total days = 1337, between 1280 and 1250 not uch difference - between 250-50+ tweets per day.

#. 2. Plot summary statistics:
# -- Using number of tweets per day as an indication, pick number of days:
fns.plot_x_last_x_days(hdfs_date_results, "MEAN_SENT", 30, "Mean sentiment 30 days", "Mean Sentiment")
fns.plot_x_last_x_days(hdfs_date_results, "STND_DEV_SENT", 30, "Standard Deviation of Sentiment 30 days", "Standard Deviation")

plt.figure()
plt.plot([i for i in range(0,30)], hdfs_date_results["MEAN_SENT"][len(hdfs_date_results)-30:])
plt.plot([i for i in range(0,30)], hdfs_date_results["STND_DEV_SENT"][len(hdfs_date_results)-30:])

plt.figure()
plt.plot([i for i in range(0,60)], hdfs_date_results["FAVS_PER_TWEETS"][len(hdfs_date_results)-60:])
plt.plot([i for i in range(0,60)], hdfs_date_results["RT_PER_TWEET"][len(hdfs_date_results)-60:])
plt.xlabel("Number of Days")
plt.ylabel()
plt.title()
plt.legend()

##########################################################
#. Analysis 2: Dataset reduced by account:
#. 1. Tweets by account = 1800
#. 2. Plotting average sentiment
#. 3. Get top 10 and bottom 10
##########################################################

#. 1. Plotting average sentiment
fns.plot_x_last_x_days_acc(hdfs_acc_results, 'MEAN_SENT', [i for i in range(0,len(hdfs_acc_results))], len(hdfs_acc_results), "Mean Sentiment per Account", "Mean Sentiment")

#. 2. Sort by sentiment:
hdfs_acc_results = hdfs_acc_results.sort_values(by='MEAN_SENT')
bottom_x_sentiment = hdfs_acc_results[0:5]
top_x_sentiment = hdfs_acc_results[len(hdfs_acc_results)-5:]

#. 2. Plot top 10 v Bottom 10:
fns.plot_x_last_x_days_acc(top_x_sentiment, 'MEAN_SENT', [i for i in top_x_sentiment['SOURCE']], len(top_x_sentiment), "Top 5 by sentiment", "Mean Sentiment")
fns.plot_x_last_x_days_acc(bottom_x_sentiment, 'MEAN_SENT', [i for i in bottom_x_sentiment['SOURCE']], len(bottom_x_sentiment), "Bottom 5 by sentiment", "Mean Sentiment")


top_10_sentiment[['SOURCE', 'MEAN_SENT']].plot(kind = 'bar', )



