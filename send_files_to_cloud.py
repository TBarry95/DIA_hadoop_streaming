#!/usr/bin/python

# SCRIPT 3: Send cleaned input data and all python scripts (2 mappers, 2 reducers) to Ubuntu AWS EC2 machine.

# DES: Script sends all files needed for analysis onto Ubuntu EC2 instance where the HDFS is installed.
#      Sends: 2 Mappers and 2 reducers, as well as input data.
# BY:  Tiernan Barry, x19141840 - NCI.

# Libraries:
import pysftp

# Installations (if needed):
# pip install pysftp

##########################################################################
# Send data to Ubuntu server in EC2:
##########################################################################

# -- ubuntu credentials:
my_hostname = "54.196.149.165"
my_username = "ubuntu"
my_password = None
cnopts = pysftp.CnOpts()
cnopts.hostkeys = None
private_key_pem = "tb_ubuntu_mint.pem"

# -- Connect to Ubuntu:
with pysftp.Connection(host=my_hostname, username=my_username, password=my_password, cnopts=cnopts, private_key=private_key_pem) as sftp:
    print("Connected to Ubuntu EC2 Server....")

    # -- 5 files for sending to Ubuntu:
    # ---- 1. Processed tweets:
    # Commenting out as not necessary each time:
    # local_path_tweets = "/home/tiernan/PycharmProjects/DIA/twitter_media_prod.csv"

    # ---- 2. Mappers: Map only, Date and Account
    local_path_mapper = "/home/tiernan/PycharmProjects/DIA/mapper_stop_words.py"
    local_path_mapper1 = "/home/tiernan/PycharmProjects/DIA/mapper_twitter_date.py"
    local_path_mapper2 = "/home/tiernan/PycharmProjects/DIA/mapper_twitter_account.py"

    # ---- 3. Reducers: Date and Account:
    local_path_reducer1 = "/home/tiernan/PycharmProjects/DIA/reducer_twitter_date.py"
    local_path_reducer2 = "/home/tiernan/PycharmProjects/DIA/reducer_twitter_account.py"

    # -- Define remote path for files:
    remote_path_tweets = '/home/hduser/mr_tests/production_scripts/twitter_data_prod.csv'
    remote_path_map = '/home/hduser/mr_tests/mapper_stop_words.py'
    remote_path_map1 = '/home/hduser/mr_tests/production_scripts/mapper_twitter_date.py'
    remote_path_map2 = '/home/hduser/mr_tests/production_scripts/mapper_twitter_account.py'
    remote_path_red1 = '/home/hduser/mr_tests/production_scripts/reducer_twitter_date.py'
    remote_path_red2 = '/home/hduser/mr_tests/production_scripts/reducer_twitter_account.py'

    #sftp.put(local_path_tweets, remote_path_tweets)
    sftp.put(local_path_mapper1, remote_path_map1)
    sftp.put(local_path_mapper2, remote_path_map2)
    sftp.put(local_path_reducer1, remote_path_red1)
    sftp.put(local_path_reducer2, remote_path_red2)
    sftp.put(local_path_mapper, remote_path_map)

    sftp.close()
    print("Connection closed")