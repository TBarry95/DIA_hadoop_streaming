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
from bokeh.layouts import column, gridplot, LayoutDOM

# Installations (if needed):
# import missingno as msno

##########################################################################
# Extract: Read in data locally (after being called from HDFS in script 7)
##########################################################################

hdfs_date_results = pd.read_csv("/home/tiernan/PycharmProjects/DIA/date_apr9.csv")
hdfs_acc_results = pd.read_csv("/home/tiernan/PycharmProjects/DIA/account_apr9.csv")

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

##########################################################
#. Analysis 1: Dataset reduced by date: date_apr8.csv
#. 1. Plot number of tweets by date.
#. 2. Based on above, plot summary statistics by date.
#. 3. Correlations?
##########################################################

#. 1. Plot number of tweets by TWEETS_PER_DATE
fns.plot_x_last_x_days(hdfs_date_results, "TWEETS_PER_DATE", len(hdfs_date_results), "Number of Tweets by Day", "Number of Tweets")
# -- Notes from above graph:
# -- Total days = 1337, using 20 days means theres around 2k+ tweets per day.
# -- Total days = 1337, using 30-40 days means theres around 600+ tweets per day.
# -- Total days = 1337, using 70 days means theres around 200+ tweets per day.
# -- Total days = 1337, using 100 days means theres around 50-100+ tweets per day.
# -- Total days = 1337, between 1280 and 1250 not uch difference - between 250-50+ tweets per day.
'''
# 2. Build Bokeh Dashboard for all Date analysis:
# -- Plot Fav:Follower vs RT:Follower by date:
fav_dt = hdfs_date_results["FAVS_PER_TWEETS"][len(hdfs_date_results)-60:]
rt_dt = hdfs_date_results["RT_PER_TWEET"][len(hdfs_date_results)-60:]
days = [i for i in range(0,60)]
fav_rt = figure(title="Fav to Follower vs RT to Follower Ratio by Date", plot_height=600,plot_width=800,
                x_axis_label = "Last 60 Days", y_axis_label = "Ratio") # x_axis_type="datetime
fav_rt.extra_y_ranges= {"RT": Range1d(start=0, end=max(rt_dt))}
fav_rt.add_layout(LinearAxis(y_range_name="RT"), "right")
fav_rt.line(days, fav_dt, legend="Fav:Follower",alpha=0.8, color="#53777a")
fav_rt.line(days, rt_dt, legend="RT:Follower", alpha=0.8, color="#c02942")
fav_rt.legend.location = "top_right"
fav_rt.legend.click_policy="hide"

fav_acc = hdfs_acc_results['FAV_TO_FLWR'][len(hdfs_acc_results)-60:]
rt_acc = hdfs_acc_results['RT_TO_FLWR'][len(hdfs_acc_results)-60:]
days = [i for i in range(0,60)]
fav_rt_acc = figure(title="Fav to Follower vs RT to Follower Ratio by Date", plot_height=600,plot_width=800,
                x_axis_label = "Last 60 Days", y_axis_label = "Ratio") #x_axis_type="datetime
fav_rt_acc.extra_y_ranges= {"RT": Range1d(start=0, end=max(rt))}
fav_rt_acc.add_layout(LinearAxis(y_range_name="RT"), "right")
fav_rt_acc.line(days, fav_acc, legend="Fav:Follower",alpha=0.8, color="#53777a")
fav_rt_acc.line(days, rt_acc, legend="RT:Follower", alpha=0.8, color="#c02942", y_range_name = "RT")
fav_rt_acc.legend.location = "top_right"
fav_rt_acc.legend.click_policy="hide"

grid = gridplot([fav_rt,fav_acc], ncols=2, plot_width=400, plot_height=300)
show(grid)'''

#twets_2020 = hdfs_date_results[hdfs_date_results['DATE'][0:4] == "2020"]

#plt.figure()
#plt.plot([i[0:4] for i in hdfs_date_results['DATE']], hdfs_date_results['TWEETS_PER_DATE'])
#hdfs_date_results[['TWEETS_PER_DATE', 'DATE']].plot(kind = 'hist')

#. 2. Plot Mean Sentiment for last X days:
fns.plot_x_last_x_days(hdfs_date_results, "MEAN_SENT", 100, "Daily Mean Sentiment 100 days", "Mean Sentiment")
fns.plot_x_last_x_days(hdfs_date_results, "MEAN_SENT", 60, "Daily Mean Sentiment  60 days", "Mean Sentiment")
fns.plot_x_last_x_days(hdfs_date_results, "MEAN_SENT", 30, "Daily Mean Sentiment  30 days", "Mean Sentiment")
fns.plot_x_last_x_days(hdfs_date_results, "MEAN_SENT", 20, "Daily Mean Sentiment  20 days", "Mean Sentiment")

fns.plot_x_last_x_days(hdfs_date_results, "MEAN_SENT", 30, "Daily Mean Sentiment  30 days", "Mean Sentiment", pct_ch="YES")

#. 3. Plot standard deviation of sentiment last X days:
fns.plot_x_last_x_days(hdfs_date_results, "STND_DEV_SENT", 30, "Standard Deviation of Sentiment 30 days", "Standard Deviation")

#. 4. Plot Daily Favourite:Followers ratio vs RT:Followers ratio by Date:
plt.figure()
plot_dt_fav, = plt.plot([i for i in range(0,60)], hdfs_date_results["FAVS_PER_TWEETS"][len(hdfs_date_results)-60:])
plot_dt_rt, = plt.plot([i for i in range(0,60)], hdfs_date_results["RT_PER_TWEET"][len(hdfs_date_results)-60:])
plt.xlabel("Number of Days")
plt.ylabel("Ratio per Day")
plt.title("Daily Favourite:Follower vs RT:Follower Ratio")
plt.legend([plot_dt_fav, plot_dt_rt], ["Mean Favourite", "Mean RT"])

#. 5. Plot Correlation between sentiment and Favourites/RT for last X days:
plt.figure()
plot_dt_corr1, = plt.plot([i for i in range(0,100)], hdfs_date_results['CORR_FAV_SENT'][len(hdfs_date_results)-100:])
plot_dt_corr2, = plt.plot([i for i in range(0,100)], hdfs_date_results['CORR_RT_SENT'][len(hdfs_date_results)-100:])
plt.xlabel("Number of Days")
plt.ylabel("Ratio per Day")
plt.title("Daily Correlation: Sentiment:Favourite vs Sentiment:RT")
plt.legend([plot_dt_corr1, plot_dt_corr2], ["Sentiment:Favourite", "Sentiment:RT"])

##########################################################
#. Analysis 2: Dataset reduced by account: account_apr8.csv
#. 1. Tweets by account = 2400
#. 2. Plotting average sentiment
#. 3. Get top 10 and bottom 10
##########################################################

# 1. Plotting average sentiment
fns.plot_x_last_x_days_acc(hdfs_acc_results, 'MEAN_SENT', [i for i in range(0,len(hdfs_acc_results))], len(hdfs_acc_results), "Mean Sentiment per Account", "Mean Sentiment")

# 2. Plot Tables of Top X accounts and Bottom X accunts:
# -- Sort by sentiment:
hdfs_acc_results = hdfs_acc_results.sort_values(by='MEAN_SENT')
bottom_x_sentiment = hdfs_acc_results[['SOURCE', 'MEAN_SENT']][0:10]
bottom_x_sentiment = bottom_x_sentiment.sort_values(by=['MEAN_SENT'], ascending=True)
top_x_sentiment = hdfs_acc_results[['SOURCE', 'MEAN_SENT']][len(hdfs_acc_results)-10:]
top_x_sentiment = top_x_sentiment.sort_values(by=['MEAN_SENT'], ascending=False)

# -- Plot bottom X accounts:
fig_bottom, ax_bottom = plt.subplots()
ax_bottom.table(cellText=bottom_x_sentiment.values, colLabels=bottom_x_sentiment.columns, loc='center')
ax_bottom.set_title("Bottom 10 Ranked Twitter Accounts by Mean Sentiment", size=15)
ax_bottom.axis('off')

bottom_x_sentiment = bottom_x_sentiment.set_index('SOURCE')
bottom_x_sentiment.plot(kind='bar')

# -- Plot top X accounts:
fig_top, ax_top = plt.subplots()
ax_top.table(cellText=top_x_sentiment.values, colLabels=top_x_sentiment.columns, loc='center')
ax_top.set_title("Top 10 Ranked Twitter Accounts by Mean Sentiment")
ax_top.axis('off')

top_x_sentiment = top_x_sentiment.set_index('SOURCE')
top_x_sentiment.plot(kind='bar')
