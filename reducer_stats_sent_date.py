#!/usr/bin/python

# DES: Reducer script to find summary statistics for sentiment analysis scores
#      finds mean, median, max, min, standard deviation for each date.

# BY:  Tiernan Barry, x19141840 - NCI.

# 1. Libraries:
from operator import itemgetter
import sys
from sortedcontainers import SortedList
import statistics as stats
import csv

# 2. reduce key,values by date and find stats per date:
last_date_key = None
sent_list_sort = SortedList()
count_per_date = 0
aggregate_sentiment = 0

#print("DATE", "MEAN", "STND_DEV", "MEDIAN", "MIN", "MAX", "COUNT")
print("DATE, SOURCE, MEAN_SENT, STND_DEV_SENT, MEDIAN_SENT, MIN_SENT, MAX_SENT, TWEETS_PER_DATE, FAVS_PER_TWEETS, RT_PER_TWEET")

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

    else:
        if last_date_key:
            print(('%s,%s,%s,%s,%s,%s,%s,%s') % (last_date_key,
                                                    source,
                                            aggregate_sentiment / count_per_date, # avg
                                            stats.stdev(sent_list_sort), # stnd dev
                                            sent_list_sort[int(len(sent_list_sort)/2)], # median
                                            sent_list_sort[0], # min
                                            sent_list_sort[-1], # max
                                            count_per_date)) #2
        aggregate_sentiment = sentiment_value
        last_date_key = this_date_key
        count_per_date = 1

# -- Output summary stats:
if last_date_key == this_date_key:
    print(('%s,%s,%s,%s,%s,%s,%s,%s') % (last_date_key,
                                                source,
                                    aggregate_sentiment / count_per_date,  # avg
                                    stats.stdev(sent_list_sort),  # stnd dev
                                    sent_list_sort[int(len(sent_list_sort) / 2)],  # median
                                    sent_list_sort[0],  # min
                                    sent_list_sort[-1],  # max
                                    count_per_date))  # 2
