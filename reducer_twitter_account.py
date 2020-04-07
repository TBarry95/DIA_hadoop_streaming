#!/usr/bin/python

# SCRIPT 6: Reducer script for mapper_twitter_account.py.

# DES: Reducer script to find insights regarding sentiment analysis scores for each account.
#      On a follower weighted basis, as well as a tweets per day basis, finds insights
#      such as average sentiment scores per date, and correlations between sentiment scores and favourites/RTs.
# BY:  Tiernan Barry, x19141840 - NCI.

# 1. Libraries:
import sys
import csv
from sortedcontainers import SortedList
import statistics as stats
from scipy.stats import pearsonr
import warnings
warnings.filterwarnings("ignore") # correlation throws warnings if result is nan due to list of constants. Need to ignore this.

# 2. reduce key,values by date and find stats per date:
last_account_key = None
sent_list_sort = SortedList()
count_per_acc = 0
aggregate_sentiment = 0
favs_per_acc = 0
rt_per_acc = 0
followers = SortedList() # max
list_sentiment = []
favs_to_follower = []
rt_to_follower = []

print("SOURCE, MEAN_SENT, STND_DEV_SENT, MEDIAN_SENT, MIN_SENT, MAX_SENT, "
      "MAX_FLWR, FAV_TO_FLWR, RT_TO_FLWR, CORR_FAV_SENT, CORR_RT_SENT, TWEETS_PER_ACC")

for key_value in csv.reader(sys.stdin):
    if len(key_value) > 0: # skips the last empty row.
        this_account_key = key_value[0]
        date = key_value[1]
        fav = int(key_value[2])
        rt = int(key_value[3])
        follower = int(key_value[4])
        sentiment_value = float(key_value[6])

        if last_account_key == this_account_key:
            count_per_acc += 1
            sent_list_sort.add(sentiment_value) #1
            aggregate_sentiment += sentiment_value
            favs_per_acc += fav # add favs per tweet, etc
            rt_per_acc += rt
            followers.add(follower)
            list_sentiment.append(sentiment_value)
            favs_to_follower.append(fav/follower)
            rt_to_follower.append(rt/follower)

        else:
            if last_account_key:
                print(('%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s') %
                      (last_account_key, # source
                       aggregate_sentiment / count_per_acc, # MEAN
                       stats.stdev(sent_list_sort), # stnd dev
                       sent_list_sort[int(len(sent_list_sort)/2)], # median
                       sent_list_sort[0], # min
                       sent_list_sort[-1], # max
                       followers[-1],
                       favs_per_acc/followers[-1], # FAV_TO_FLWR ratio
                       rt_per_acc/followers[-1], # RT_TO_FLWR ratio
                       pearsonr(list_sentiment, favs_to_follower)[0],
                       pearsonr(list_sentiment, rt_to_follower)[0],
                       count_per_acc)) # count tweets under analysis

            aggregate_sentiment = sentiment_value
            last_account_key = this_account_key
            count_per_acc = 1 # restart list for each account
            favs_per_acc = fav # restart list for each account
            rt_per_acc = rt # restart list for each account
            sent_list_sort = SortedList()
            sent_list_sort.add(sentiment_value)
            followers = SortedList()
            followers.add(follower)
            list_sentiment = []
            favs_to_follower = []
            rt_to_follower = []
            list_sentiment.append(sentiment_value)
            favs_to_follower.append(fav/follower)
            rt_to_follower.append(rt/follower)

# -- Output summary stats:
if last_account_key == this_account_key:
    print(('%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s') %
          (last_account_key,  # source
          aggregate_sentiment / count_per_acc,  # MEAN
          stats.stdev(sent_list_sort),  # stnd dev
          sent_list_sort[int(len(sent_list_sort) / 2)],  # median
          sent_list_sort[0],  # min
          sent_list_sort[-1],
          followers[-1],  # max
          favs_per_acc / followers[-1],  # FAV_TO_FLWR ratio
          rt_per_acc / followers[-1],  # RT_TO_FLWR ratio
          pearsonr(list_sentiment, favs_to_follower)[0],
          pearsonr(list_sentiment, rt_to_follower)[0],
          count_per_acc))  # count tweets under analysis