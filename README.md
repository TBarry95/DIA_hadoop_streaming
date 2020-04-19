# Reproducing Twitter Sentiment Analysis using Hadoop: 

1. Create new Conda environment using env_dia.yml file included in repo:
  - env_dia.yml file outlines all dependencies required to fully reproduce environment. 
  - By using env_dia.yml to create the conda environment, you can avoid having to rerun the code numerous times until everything is  	        installed. 
  - For further information on creating conda environment using .yml file, visit:
     https://docs.conda.io/projects/conda/en/latest/user-guide/tasks/manage-environments.html?fbclid=IwAR3tWUCr1qIK4BmHfcQ-uRrkkSEDjiRwREMAIM4089dphzgntg66F-apuU8#creating-an-environment-from-an-environment-yml-file

2. Add private key to working directoy:
  - Private .pem file must manually be added to working directory.  
  - This connects local machine to remote ubuntu server for put and get sftp requests. 
  - File is provided in zip folder from moodle submission (tb_ubuntu_mint.pem)
  
3. From terminal, run main.py: 
  - main.py file runs all possible scripts. Possible scripts include: 
      1. Gathering tweets from tweepy. 
           - full dataset takes 3 hours to run (150k+ tweets)
           - for testing, this only collects 1 page per account and ignores the result.
           - raw dataset of 150k+ tweets is provided in repo and is picked up by rest of code. 
      2. Preprocessing tweets
      3. Sending to AWS ubuntu machine. 
      4. Collecting HDFS results. 
      5. Visualising results. 
      
# Please note: 

1. The execution of Hadoop Streaming jobs is not automated within the main.py file. 
   The results were last collected on April 12th, and are used for the final report and for 
   reproducing this analysis. To rerun the Hadoop jobs: 
   - Login to the Ubuntu remote server using .pem file
   - Login as hduser (hadoop)
   - cd /usr/local/hadoop/share/hadoop/tools/lib/ 
   - Change dates on output files from hadoop_streaming_jobs.txt 
   - Copy results back to Ubuntu from HDFS and update get_data_from_cloud.py script to correct locations
   
2. Hadoop streaming commands are provided in the repo within a txt file. 

