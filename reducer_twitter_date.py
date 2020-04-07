#!/usr/bin/python

# SCRIPT 4: Reducer script for

# DES: Reducer script to find summary statistics for sentiment analysis scores
#      finds mean, median, max, min, standard deviation for each date.

# BY:  Tiernan Barry, x19141840 - NCI.

# 1. Libraries:
from operator import itemgetter
import sys
from sortedcontainers import SortedList
import statistics as stats
import csv
from scipy.stats import pearsonr

# 2. reduce key,values by date and find stats per date:
last_date_key = None
sent_list_sort = SortedList()
count_per_date = 0
favs_per_dt = 0
rt_per_dt = 0
aggregate_sentiment = 0
list_sentiment = []
favs_to_follower = []
rt_to_follower = []

#print("DATE", "MEAN", "STND_DEV", "MEDIAN", "MIN", "MAX", "COUNT")
print("DATE, SOURCE, MEAN_SENT, STND_DEV_SENT, MEDIAN_SENT, MIN_SENT, MAX_SENT, FAVS_PER_TWEETS, RT_PER_TWEET, "
      "CORR_FAV_SENT, CORR_RT_SENT,TWEETS_PER_DATE")

for key_value in csv.reader(sys.stdin):
    this_date_key = key_value[0]
    source = key_value[1]
    fav = int(key_value[2])
    rt = int(key_value[3])
    follower = int(key_value[4])
    sentiment_value = float(key_value[6])

    if last_date_key == this_date_key:
        count_per_date += 1
        sent_list_sort.add(sentiment_value) #1
        aggregate_sentiment += sentiment_value
        favs_per_dt += fav  # add favs per date
        rt_per_dt += rt
        list_sentiment.append(sentiment_value)
        favs_to_follower.append(fav / follower)
        rt_to_follower.append(rt / follower)

    else:
        if last_date_key:
            print(('%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s') % (last_date_key,
                                                             source,
                                                            aggregate_sentiment / count_per_date, # avg
                                                            stats.stdev(sent_list_sort), # stnd dev
                                                            sent_list_sort[int(len(sent_list_sort)/2)], # median
                                                            sent_list_sort[0], # min
                                                            sent_list_sort[-1], # max
                                                            favs_per_dt / count_per_date, # favs:number tweet ratio
                                                            rt_per_dt / count_per_date, # rt:number twe
                                                             pearsonr(list_sentiment, favs_to_follower)[0],
                                                             pearsonr(list_sentiment, rt_to_follower)[0],
                                                            count_per_date)) #2
        aggregate_sentiment = sentiment_value
        last_date_key = this_date_key
        favs_per_dt = fav  # restart list for each account
        rt_per_dt = rt  # restart list for each account
        count_per_date = 1

# -- Output summary stats:
if last_date_key == this_date_key:
    print(('%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s') % (last_date_key,
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
                                                count_per_date))  # 2
