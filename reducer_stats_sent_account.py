#!/usr/bin/python

# DES: Reducer script to find summary statistics for sentiment analysis scores.
#      Finds follower weighted statistics, perhaps exposing link to sentiment.

# BY:  Tiernan Barry, x19141840 - NCI.

# 1. Libraries:
from operator import itemgetter
import sys
import csv
from sortedcontainers import SortedList
import statistics as stats

# 2. reduce key,values by date and find stats per date:
last_account_key = None
sent_list_sort = SortedList()
count_per_date = 0
aggregate_sentiment = 0

print("DATE_SOURCE, MEAN, STND_DEV, MEDIAN, MIN, MAX, COUNT")

for key_value in csv.reader(sys.stdin):
    if len(key_value) > 0: # skips the last empty row.
        this_account_key = key_value[1]
        sentiment_value = float(key_value[5])

        if last_account_key == this_account_key:
            count_per_date += 1
            sent_list_sort.add(sentiment_value) #1
            aggregate_sentiment += sentiment_value

        else:
            if last_account_key:
                print(('%s,%s,%s,%s,%s,%s,%s') % (last_account_key,
                                                aggregate_sentiment / count_per_date, # avg
                                                stats.stdev(sent_list_sort), # stnd dev
                                                sent_list_sort[int(len(sent_list_sort)/2)], # median
                                                sent_list_sort[0], # min
                                                sent_list_sort[-1], # max
                                                count_per_date)) #2
            aggregate_sentiment = sentiment_value
            last_account_key = this_account_key
            count_per_date = 1

# -- Output summary stats:
if last_account_key == this_account_key:
    print(('%s,%s,%s,%s,%s,%s,%s') % (last_account_key,
                                    aggregate_sentiment / count_per_date,  # avg
                                    stats.stdev(sent_list_sort),  # stnd dev
                                    sent_list_sort[int(len(sent_list_sort) / 2)],  # median
                                    sent_list_sort[0],  # min
                                    sent_list_sort[-1],  # max
                                    count_per_date))'''