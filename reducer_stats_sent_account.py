#!/usr/bin/python

# DES: Reducer script to find summary statistics for sentiment analysis scores,
#      while also finding follower weighted statistics for each account.
#      These results are to be analysed to expose any patterns between sentiment and statistics.
#      Key = Account, Value = Data
# BY:  Tiernan Barry, x19141840 - NCI.

# 1. Libraries:
import sys
import csv
from sortedcontainers import SortedList
import statistics as stats

# 2. reduce key,values by date and find stats per date:
last_account_key = None
sent_list_sort = SortedList()
count_per_acc = 0
aggregate_sentiment = 0
favs_per_acc = 0
rt_per_acc = 0
followers = SortedList() # max

print("SOURCE, MEAN, STND_DEV, MEDIAN, MIN, MAX, MIN_FLWR, MAX_FLWR, FAV_TO_FLWR, RT_TO_FLWR, COUNT")

for key_value in csv.reader(sys.stdin):
    if len(key_value) > 0: # skips the last empty row.
        this_account_key = key_value[1]
        fav = int(key_value[2])
        rt = int(key_value[3])
        follower = int(key_value[4])
        sentiment_value = float(key_value[5])

        if last_account_key == this_account_key:
            count_per_acc += 1
            sent_list_sort.add(sentiment_value) #1
            aggregate_sentiment += sentiment_value
            favs_per_acc += fav # add favs per tweet, etc
            rt_per_acc += rt
            followers.add(follower)

        else:
            if last_account_key:
                print(('%s,%s,%s,%s,%s,%s,%s,%s,%s,%s') % (last_account_key, # source
                                                aggregate_sentiment / count_per_acc, # MEAN
                                                stats.stdev(sent_list_sort), # stnd dev
                                                sent_list_sort[int(len(sent_list_sort)/2)], # median
                                                sent_list_sort[0], # min
                                                sent_list_sort[-1], # max
                                                followers[-1],
                                                favs_per_acc/followers[-1], # FAV_TO_FLWR ratio
                                                rt_per_acc/followers[-1], # RT_TO_FLWR ratio
                                                count_per_acc)) # count tweets under analysis
            aggregate_sentiment = sentiment_value
            last_account_key = this_account_key
            count_per_acc = 1 # restart list for each account
            favs_per_acc = fav # restart list for each account
            rt_per_acc = rt # restart list for each account
            followers = SortedList()
            followers.add(follower)

# -- Output summary stats:
if last_account_key == this_account_key:
    print(('%s,%s,%s,%s,%s,%s,%s,%s,%s,%s') % (last_account_key,  # source
                                            aggregate_sentiment / count_per_acc,  # MEAN
                                            stats.stdev(sent_list_sort),  # stnd dev
                                            sent_list_sort[int(len(sent_list_sort) / 2)],  # median
                                            sent_list_sort[0],  # min
                                            sent_list_sort[-1],
                                            followers[-1],  # max
                                            favs_per_acc / followers[-1],  # FAV_TO_FLWR ratio
                                            rt_per_acc / followers[-1],  # RT_TO_FLWR ratio
                                            count_per_acc))  # count tweets under analysis