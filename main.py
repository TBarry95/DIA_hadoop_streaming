#!/usr/bin/python

# DES: Main script for project execution.
#      By importing each file, the script is executed in correct sequence.
#      Running Hadoop jobs is not included, and needs to be manually ran from EC2 machine (scripts 4,5,6,7,8)
#      Scripts 1, 2 and 3 extract and prepare the data for the Hadoop jobs.
#      Scripts 9 and 10 extract data from EC2 machine, and analyse map-reduce results.
# BY: Tiernan BArry - x19141840

# 1. Get tweets
# -- Comment this out unless wanting to refresh dataset - takes 3 hours
# -- Recommended: read in twitter_mass_media_data.csv instead to working directory
# import get_twitter_data

# 2. Preprocess tweets and send to cloud
import process_tweets

# 3.
import send_files_to_cloud

# 4-8: Mappers and reducers all ran from cloud machine manually.

# 9.
import get_data_from_cloud

# 10.
import analyse_hdfs_results













