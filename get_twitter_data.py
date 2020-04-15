#!/usr/bin/python

# SCRIPT 1: Source Twitter datasets

# DES: Source script for extracting Twitter dataset for Data Intensive Architecture - Map Reduce project.
#      Functions for extracting datasets are defined in functions_tweet_mapreduce.py, and called within this script.
#      All installations and libraries needed are called in functions_tweet_mapreduce.py.
# BY: Tiernan Barry - x19141840 - NCI.

# Libraries and Imported files:
import functions_tweet_mapreduce as fns

######################################################################################
# Extract: 1. Twitter data from global media twitter pages.
######################################################################################

twitter_pgs = ["CNN", "BBCWorld", "BBCBreaking", "BBCNews", "ABC", "Independent",
               "RTENewsNow", "Independent_ie", "guardian", "guardiannews", "rtenews", "thejournal_ie",
               "wef", "IMFNews", "WHO", "euronews", "MailOnline", "TheSun", "Daily_Express", "DailyMirror",
               "standardnews", "LBC", "itvnews", "thetimes", "IrishTimes", "ANI", "XHNews", "TIME", "OANN",
               "BreitbartNews", "Channel4News", "BuzzFeedNews", "NewstalkFM", "NBCNewsBusiness", "CNBCnow",
               "markets", "YahooFinance", "MarketWatch", "Forbes", "businessinsider", "thehill", "CNNPolitics",
               "NPR", "AP", "USATODAY", "NYDailyNews", "nypost", "BBCLondonNews", "DailyMailUK",
               "CBSNews", "MSNBC", "nytimes", "FT", "business", "cnni", "RT_com", "AJEnglish", "CBS", "NewsHour",
               "BreakingNews", "cnnbrk", "WSJ", "Reuters", "SkyNews", "CBCAlerts"]

# -- Takes 2-3 hours... Test by changing number of pages from 120 to 1 or 2.
tweets_list = fns.get_tweets_list(twitter_pgs, 120) # 155k + tweets

df_all_tweets = fns.tweets_to_df(tweets_list)

df_all_tweets = df_all_tweets.sort_values(by='DATE_TIME', ascending=0)

df_all_tweets = df_all_tweets.drop_duplicates()

######################################################################################
# Load: Write to project directory:
######################################################################################

# df_all_tweets.to_csv("./twitter_mass_media_data.csv", index= False)

# -- Data cleaned and processed for MapReduce job in Script 2: process_tweets.py