
# coding: utf-8

# In[78]:

# As written in instructions all libraries are allowed except SparkSQL therefore I will be using PySpark and Pandas for my
# data wrangling.
import pandas as pd
import re
import time
import datetime
import numpy as np


# In[57]:
# Assuming spark context is defined 'sc'.

input_file = sc.textFile('2015_07_22_mktplace_shop_web_log_sample.log')


# In[107]:


def get_log_stats(line):
    
#Function for extracting timestamp client_ip and destination_ip. The destination_ip is used to represent distinct URL's
    try:
        timestamp = re.search('\d{4}-\d{2}-\d{2}T\d{2}\:\d{2}:\d{2}',line).group().replace('T',' ') # yyyy/mm/dd HH:MM:SS
        ips = re.findall('\d+\.\d+\.\d+\.\d+',line) #Regex for extracting ip addresses
        visitor_ip = ips[0]
        url_ip = ips[1]
        return [timestamp,visitor_ip,url_ip]
    except:
        #Returning Gateway Timeouts as in the url column
        return [timestamp,visitor_ip,'504']
    

splitted_input = input_file.map(get_log_stats)
logs_pd = splitted_input.toDF().toPandas() #Converting to Pandas dataframe, takes 30 seconds approx for whole file.


# In[110]:

logs_pd.columns = ['timestamp','visitor_ip','destination_ip'] #Renaming columns of DF

#To find out number of 504s and for which visitors that we have encountered it,appending .count() will give us the total count

logs_pd[logs_pd.destination_ip == '504']


# In[111]:

# Filtering out the 504 entries

logs_pd_sorted_cleaned = logs_pd.drop(logs_pd[logs_pd.destination_ip  == '504'].index)

#Sorting by visitor_ip inplace to bring all individual user hits together

logs_pd_sorted_cleaned.sort(columns = ['visitor_ip'],axis = 0,inplace = True)

#Converting date string to unix timestamp for convenience in sessionizing

logs_pd_sorted_cleaned['timestamp'] = logs_pd_sorted_cleaned.timestamp \
													.map(lambda x: time.mktime(datetime.datetime.strptime(x, "%Y-%m-%d %H:%M:%S").timetuple()))

logs_pd_sorted_cleaned


# In[122]:

#Grouping by visitor_ip

logs_pd_sorted_cleaned_g = logs_pd_sorted_cleaned.groupby('visitor_ip')

#Creating session numbers for each individual user based on a session length of 15 min= 900seconds on basis on timestamp

logs_pd_sorted_cleaned['session_number'] = logs_pd_sorted_cleaned_g['timestamp'].apply(lambda s: (s - s.shift(1) > 900)                                            .fillna(0).cumsum(skipna=False))
logs_pd_sorted_cleaned


# In[114]:

# Calculating the average session time by first calculating the average session time per 
#user and then calculating a mean of those values
avg =logs_pd_sorted_cleaned.groupby(['visitor_ip','session_number'])['timestamp'].apply(lambda x: max(x)-min(x)).reset_index()
avg.columns = ['visitor_ip','session_number','average_time_per_session_per_user']
avg


# In[124]:

#Calculating overall average session time (Comes out to be 1481 seconds ~ 25 mins)
overall_average_session_time = avg.groupby('visitor_ip')\
													.agg({'average_time_per_session_per_user' : np.mean})['average_time_per_session_per_user'].mean()


# In[125]:

#Finding ip's of most engaged users by finding longest mean session times, this is done by sorting in descending after averaging

copy_df = avg.groupby('visitor_ip').agg({'average_time_per_session_per_user' : np.mean})['average_time_per_session_per_user']
most_engaged_users = copy_df.reset_index().sort('average_time_per_session_per_user',ascending = False)


# In[126]:

#unique url session numbers: 1. Apply groupby to get unique destination_ip per user per session
#                            2. Perform a count and again groupby to get count of unique urls visited per session

unique_urls_per_session = logs_pd_sorted_cleaned.groupby(['visitor_ip','session_number','destination_ip']) \
													.count().reset_index().groupby(['visitor_ip','session_number']) \
													.count('destination_ip').reset_index()
#Renaming and dropping extra columns
unique_urls_per_session.columns = ['visitor_ip','session_number','unique_url','timestamp']
unique_urls_per_session = unique_urls_per_session.drop('timestamp',axis=1)

#Result is Number of unqiue url hits per session per user
#IP Address does not guarantee distinct user therefore if we had user_id embedded withing the url then 
#further distinctive analysis could have been done


# # ANSWERS

# In[134]:

unique_urls_per_session.to_csv('unique_urls_per_session.csv', sep=',', encoding='utf-8',headers = True)
#See csv file for list


# In[128]:

overall_average_session_time 
#1480 s = 25 mins


# In[133]:

most_engaged_users.reset_index().to_csv('most_engaged_users.csv', sep=',', encoding='utf-8',headers = True)
#See csv file for list


# In[ ]:



