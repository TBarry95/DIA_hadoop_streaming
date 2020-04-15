#!/usr/bin/python

# SCRIPT 10: Analyse HDFS output, provide visualisations and insights.

# DES: Having completed 2 sentiment analyses using hadoop, this script
#      reads in the results of the analysis to highlight insights, and to visualise results.
# BY:  Tiernan Barry, x19141840 - NCI.

# Libraries:
import pandas as pd
import functions_tweet_mapreduce as fns
import matplotlib.pyplot as plt
from tabulate import tabulate
from matplotlib.pyplot import ion
ion() # enables interactive mode

# Installations (if needed):
# import missingno as msno

##########################################################################
# Extract: Read in data locally (after being called from HDFS in script 7)
##########################################################################

hdfs_date_results = pd.read_csv("./date_apr9.csv")
hdfs_acc_results = pd.read_csv("./account_apr9.csv")

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
#. 3. Plot standard deviation of sentiment last X days:
#. 4. Plot Daily Favourite:Followers ratio vs RT:Followers ratio by Date:
#. 4. Plot Daily Favourite:Followers ratio vs RT:Followers ratio by Date:
##########################################################

#. 1. Plot number of tweets by TWEETS_PER_DATE
fns.plot_x_last_x_days(hdfs_date_results, "TWEETS_PER_DATE", len(hdfs_date_results), "Number of Tweets by Day", "Number of Tweets")
# -- Notes from above graph:
# -- Total days = 1337, using 20 days means theres around 2k+ tweets per day.
# -- Total days = 1337, using 30 days means theres around 1300+ tweets per day.
# -- Total days = 1337, using 40 days means theres around 500 tweets per day.
# -- Total days = 1337, using 70 days means theres around 200+ tweets per day.
# -- Total days = 1337, using 100 days means theres around 50-100+ tweets per day.
# -- Total days = 1337, between 1280 and 1250 not much difference - between 250-50+ tweets per day.

#. 2. Plot Mean Sentiment for last X days:
fns.plot_x_last_x_days(hdfs_date_results, "MEAN_SENT", 100, "Daily Mean Sentiment 100 days", "Mean Sentiment")
fns.plot_x_last_x_days(hdfs_date_results, "MEAN_SENT", 60, "Daily Mean Sentiment  60 days", "Mean Sentiment")
fns.plot_x_last_x_days(hdfs_date_results, "MEAN_SENT", 30, "Daily Mean Sentiment  30 days", "Mean Sentiment")
fns.plot_x_last_x_days(hdfs_date_results, "MEAN_SENT", 20, "Daily Mean Sentiment  20 days", "Mean Sentiment")
fns.plot_x_last_x_days(hdfs_date_results, "MEAN_SENT", 45, "Daily % Change Mean Sentiment 40 days", "% Change Mean Sentiment", pct_ch="YES")

#. 3. Plot standard deviation of sentiment last X days:
fns.plot_x_last_x_days(hdfs_date_results, "STND_DEV_SENT", 100, "Standard Deviation of Sentiment 30 days", "Standard Deviation")

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

hdfs_date_results_dt_corr = hdfs_date_results.sort_values(['CORR_RT_SENT'])[hdfs_date_results['TWEETS_PER_DATE'] > 250]
hdfs_date_results_dt_corr = hdfs_date_results_dt_corr[['DATE', 'CORR_RT_SENT', 'TWEETS_PER_DATE']]

#-- Plot days where > 250 tweets:
fig_corr_dts, ax_corr_dt = plt.subplots()
ax_corr_dt.table(cellText=hdfs_date_results_dt_corr.values, colLabels=hdfs_date_results_dt_corr.columns, loc='center')
ax_corr_dt.set_title("Correlation between", size=15)
ax_corr_dt.axis('off')

##########################################################
#. Analysis 2: Dataset reduced by account: account_apr8.csv
#. -- Tweets by account = 2400
# 1. Plotting average sentiment
# 2. Plot Tables of Top X accounts and Bottom X accunts by sentiment:
# 3. Analyse engagements by account:
##########################################################

# 1. Plotting average sentiment
fns.plot_x_last_x_days_acc(hdfs_acc_results, 'MEAN_SENT', [i for i in range(0,len(hdfs_acc_results))], len(hdfs_acc_results), "Mean Sentiment per Account", "Mean Sentiment")
print(tabulate(hdfs_acc_results, headers=hdfs_acc_results.columns))

# 2. Plot Tables of Top X accounts and Bottom X accunts by sentiment:
# -- Sort by sentiment:
hdfs_acc_results_mean = hdfs_acc_results.sort_values(by='MEAN_SENT')
bottom_x_sentiment = hdfs_acc_results_mean[['SOURCE', 'MEAN_SENT']][0:10]
bottom_x_sentiment = bottom_x_sentiment.sort_values(by=['MEAN_SENT'], ascending=True)
top_x_sentiment = hdfs_acc_results_mean[['SOURCE', 'MEAN_SENT']][len(hdfs_acc_results)-10:]
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

top_x_sentiment[0:3].plot(kind='bar')
bottom_x_sentiment[0:3].plot(kind='bar')

# 3. Analyse engagements by account:
# -- Sort by CORR_RT_SENT:
hdfs_acc_results_corr = hdfs_acc_results.sort_values(['CORR_RT_SENT'])

median_corr = hdfs_acc_results_corr['CORR_RT_SENT'][int(len(hdfs_acc_results_corr)/2)]

bottom_x_rt_corr = hdfs_acc_results_corr[['SOURCE', 'CORR_RT_SENT']][0:10]
bottom_x_rt_corr = bottom_x_rt_corr.sort_values(['CORR_RT_SENT'], ascending=True)
top_x_rt_corr = hdfs_acc_results_corr[['SOURCE', 'CORR_RT_SENT']][len(hdfs_acc_results)-10:]
top_x_rt_corr = top_x_rt_corr.sort_values(['CORR_RT_SENT'], ascending=False)

# -- Plot bottom X accounts:
fig_corr_botm, ax_corr_botm = plt.subplots()
ax_corr_botm.table(cellText=bottom_x_rt_corr.values, colLabels=bottom_x_rt_corr.columns, loc='center')
ax_corr_botm.set_title("Lowest Correlation between Sentiment and Engagement (RT)", size=15)
ax_corr_botm.axis('off')

# -- Plot top X accounts:
fig_corr_top, ax_corr_top = plt.subplots()
ax_corr_top.table(cellText=top_x_rt_corr.values, colLabels=top_x_rt_corr.columns, loc='center')
ax_corr_top.set_title("Highest Correlation between Sentiment and Engagement (RT)")
ax_corr_top.axis('off')

top_x_rt_corr = top_x_rt_corr.set_index('SOURCE')
top_x_rt_corr.plot(kind='bar')

hdfs_acc_results_fav = hdfs_acc_results.sort_values(['CORR_FAV_SENT'])

median_fav = hdfs_acc_results_fav['CORR_FAV_SENT'][int(len(hdfs_acc_results_corr)/2)]

bottom_x_fav = hdfs_acc_results_fav[['SOURCE', 'CORR_FAV_SENT']][0:10]
bottom_x_fav = bottom_x_fav.sort_values(['CORR_FAV_SENT'], ascending=True)
top_x_rt_fav = hdfs_acc_results_fav[['SOURCE', 'CORR_FAV_SENT']][len(hdfs_acc_results)-10:]
top_x_rt_fav = top_x_rt_fav.sort_values(['CORR_FAV_SENT'], ascending=False)

# -- Plot bottom X accounts:
fig_corr_botm, ax_corr_botm = plt.subplots()
ax_corr_botm.table(cellText=bottom_x_rt_corr.values, colLabels=bottom_x_rt_corr.columns, loc='center')
ax_corr_botm.set_title("Lowest Correlation between Sentiment and Engagement (RT)", size=15)
ax_corr_botm.axis('off')
