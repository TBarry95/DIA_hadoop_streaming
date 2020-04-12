#!/usr/bin/python

# SCRIPT 4: Reducer script for mapper_twitter_date.py.

# DES: Reducer script to find insights regarding sentiment analysis scores for each date.
#      On a follower weighted basis, as well as a tweets per day basis, finds insights
#      such as average sentiment scores per date, and correlations between sentiment scores and favourites/RTs.
#      Please note:
#      Because standard deviation require at least 2 values per date, and because there are some dates with only 1 tweet,
#      this reducer adds a 0 to all lists to fulfill these requirements. The assumption is that dates with only 2 tweets these
#      values are meaningless anyway, while others they will not impact.
#      Similarily, when only 1 tweet is couneted per date, Scipy prints out a warning when calculating correlation coefficient.
#      This is why warnings are disabled, as the return is 'nan' for such dates.
#      Results are manually copied from HDFS into the Ubuntu EC2 machine using 'hdfs dfs -copyToLocal'.

# BY:  Tiernan Barry, x19141840 - NCI.

# 1. Libraries:
from operator import itemgetter
import sys
from sortedcontainers import SortedList
import statistics as stats
import csv
from scipy.stats import pearsonr

# 2. Initialise values for reduceing by date:
last_date_key = None
count_per_date = 0
favs_per_dt = 0
rt_per_dt = 0
aggregate_sentiment = 0
sent_list_sort = SortedList()
list_sentiment = []
favs_to_follower = []
rt_to_follower = []

# -- Add 0 to all lists to begin with, makes all lists at least 2 in lengths.
# -- Enables correlation and standard deviation where date < 2 (not meaningful anyway).
sent_list_sort.add(0)
list_sentiment.append(0)
favs_to_follower.append(0)
rt_to_follower.append(0)

# print(('%s,%s,%s,%s,%s,%s,%s') % (date, "MEDIA", fav_count, rt_count, followers, login_device, sentiment))

# 3. Print column headings for output in CSV format:
print("DATE, SOURCE, MEAN_SENT, STND_DEV_SENT, MEDIAN_SENT, MIN_SENT, MAX_SENT, FAVS_PER_TWEETS, RT_PER_TWEET, "
      "CORR_FAV_SENT, CORR_RT_SENT,TWEETS_PER_DATE")

# 4. Reduce by date:
for key_value in csv.reader(sys.stdin):
    this_date_key = key_value[0]
    source = key_value[1]
    fav = int(key_value[2])
    rt = int(key_value[3])
    follower = int(key_value[4])
    sentiment_value = float(key_value[6])

    if last_date_key == this_date_key:
        count_per_date += 1
        aggregate_sentiment += sentiment_value
        favs_per_dt += fav  # add favs per date
        rt_per_dt += rt
        sent_list_sort.add(sentiment_value) #1
        list_sentiment.append(sentiment_value)
        favs_to_follower.append(fav/follower)
        rt_to_follower.append(rt/follower)

    else:
        if last_date_key:
            print(('%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s') %
                  (last_date_key,
                   source,
                   aggregate_sentiment / count_per_date,  # avg
                   stats.stdev(sent_list_sort),  # stnd dev
                   sent_list_sort[int(len(sent_list_sort) / 2)], # median
                   sent_list_sort[0],  # min
                   sent_list_sort[-1],  # max
                   favs_per_dt / count_per_date, # favs:number tweet ratio
                   rt_per_dt / count_per_date,  # rt:number tweets
                   pearsonr(list_sentiment, favs_to_follower)[0],
                   pearsonr(list_sentiment, rt_to_follower)[0],
                   count_per_date))  # 2

        # Start the reducer / restart values for each iteration
        aggregate_sentiment = sentiment_value
        last_date_key = this_date_key
        favs_per_dt = fav
        rt_per_dt = rt
        count_per_date = 1
        sent_list_sort = SortedList()
        list_sentiment = []
        favs_to_follower = []
        rt_to_follower = []
        # -- Add 0 to all lists to begin with, makes all lists at least 2 in lengths:
        sent_list_sort.add(0)
        list_sentiment.append(0)
        favs_to_follower.append(0)
        rt_to_follower.append(0)
        # -- Add actual data:
        sent_list_sort.add(sentiment_value)
        list_sentiment.append(sentiment_value)
        favs_to_follower.append(fav / follower)
        rt_to_follower.append(rt / follower)

# -- Output summary stats:
if last_date_key == this_date_key:
    print(('%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s') %
           (last_date_key,
            source,
            aggregate_sentiment / count_per_date,  # avg
            stats.stdev(sent_list_sort),  # stnd dev
            sent_list_sort[int(len(sent_list_sort) / 2)],  # median
            sent_list_sort[0],  # min
            sent_list_sort[-1],  # max
            favs_per_dt / count_per_date,  # favs:number tweet ratio
            rt_per_dt / count_per_date,  # rt:number tweets
            pearsonr(list_sentiment, favs_to_follower)[0],
            pearsonr(list_sentiment, rt_to_follower)[0],
            count_per_date))