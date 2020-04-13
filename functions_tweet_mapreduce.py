#!/usr/bin/python

# DES: Support script for defining functions.
# BY:  Tiernan Barry, x19141840 - NCI.

# Libraries:
import pandas as pd
from wordcloud import WordCloud
import Twitter_API_Module as twt
import matplotlib.pyplot as plt

# # # # # # # # # # # # #
# Extract:
# # # # # # # # # # # # #
def get_tweets_list(list_of_twitter_accs, num_pages):
    tweet_list = []
    for i in list_of_twitter_accs:
        tweet_list.append(twt.TwitterClientClass(twit_user=i).get_timeline_pages(num_pages))

    all = []
    for i in tweet_list:
        for ii in i:
            for iii in ii:
                all.append(iii)
    return all

def get_irish_tweets_list(country,num_pages):
    tweet_list = []
    tweets = twt.TwitterClientClass().get_location_tweets(country, num_pages)
    for i in tweets:
        tweet_list.append(i)
    return tweet_list

def tweets_to_df(all):
    df_all_tweets = pd.DataFrame()
    df_all_tweets['TWEET_ID'] = [i.id for i in all]
    df_all_tweets['DATE_TIME'] = [str(i.created_at)[0:10] for i in all]
    df_all_tweets['TWITTER_ACC'] = [i.user.name for i in all]
    df_all_tweets['STR_ID'] = [i.id_str for i in all]
    df_all_tweets['FULL_TEXT'] = [i.full_text for i in all]
    df_all_tweets['HASHTAGS'] = [i.entities['hashtags'] for i in all]
    df_all_tweets['SOURCE'] = [i.source for i in all]
    df_all_tweets['FAV_COUNT'] = [i.favorite_count for i in all]
    df_all_tweets['RT_COUNT'] = [i.retweet_count for i in all]
    df_all_tweets['FOLLOWERS'] = [i.user.followers_count for i in all]
    df_all_tweets['TWEET_COUNT'] = [i.author.statuses_count for i in all]
    df_all_tweets['REPLY_TO_USER_ID'] = [i.in_reply_to_user_id for i in all]
    df_all_tweets['REPLY_TO_USER'] = [i.in_reply_to_screen_name for i in all]
    df_all_tweets['LEN_TWEET'] = [len(i) for i in df_all_tweets['FULL_TEXT']]
    df_all_tweets.sort_values(by='DATE_TIME', ascending=0)
    return df_all_tweets

# # # # # # # # # # # # #
# Explore Data:
# # # # # # # # # # # # #

def get_wordcloud(trump_df_cln, filepath):
    make_string = ','.join(list(trump_df_cln['PROCESSED_TEXT'].values))
    word_cloud_obj = WordCloud(background_color="white", width=550, height=550, max_words=100, contour_width=2, contour_color='steelblue')
    word_cloud_obj.generate(make_string)
    word_cloud_obj.to_file(filepath)
    return word_cloud_obj.generate(make_string)

def plot_x_last_x_days(data, stat, days, title, ylabel, pct_ch = None):
    hdfs_results_plot90 = data[len(data)-days:len(data)]
    # -- yy-mm
    #date_plot_format = [i[2:7] for i in hdfs_results_plot90['DATE']]
    plt.figure()
    if pct_ch == "YES":
        plt.plot([i for i in range(1,len(hdfs_results_plot90[stat])+1)], [i*100 for i in hdfs_results_plot90[stat].pct_change()])
        plt.xlabel("Number of Days")
        plt.ylabel(ylabel)
        plt.title(title)
    else:
        plt.plot([i for i in range(1, len(hdfs_results_plot90[stat]) + 1)], hdfs_results_plot90[stat])
        plt.xlabel("Number of Days")
        plt.ylabel(ylabel)
        plt.title(title)
    return plt.show()

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




