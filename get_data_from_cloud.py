#!/usr/bin/python

# SCRIPT 7: Get data from Ubuntu EC2 instance (data was manually copied from HDS to Ubuntu EC2), and write to local VM.

# DES: After running HDFS job, data was copied from HDFS to Ubuntu EC2 machine into hduser.
#      This script accesses Ubuntu EC2 machine by SFTP and writes results to project directory.
# BY:  Tiernan Barry, x19141840 - NCI.

# Libraries:
import pysftp

# Installations (if needed):
# pip install pysftp

##########################################################################
# Extract: Get data from Ubuntu EC2:
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

    # -- Define remote path for files:
    remote_path_account = '/home/hduser/mr_tests/hdfs_output/account_apr8.csv'
    remote_path_date = '/home/hduser/mr_tests/hdfs_output/date_apr8.csv'

    # -- Define local paths for writing:
    local_path_account = "/home/tiernan/PycharmProjects/DIA/account_apr8.csv"
    local_path_date = "/home/tiernan/PycharmProjects/DIA/date_apr8.csv"

    df_acc = sftp.get(remote_path_account, local_path_account)
    df_dt = sftp.get(remote_path_date, local_path_date)

    sftp.close()
    print("Connection closed")