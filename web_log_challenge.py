
# coding: utf-8

# In[2]:

# As written in instructions all libraries are allowed except SparkSQL therefore I will be using PySpark and Pandas for my
# data wrangling.
import pandas as pd
import re
import time
import datetime
import numpy as np


# In[3]:

def read_and_extract_fields(path):
    input_file = sc.textFile(path)
    return input_file
    
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

    
    


# In[4]:

#Optional_method:To find out list of ips which got 504s ,appending .count() will give us the total count

def get_504_response_entries(df):
    return df[df.destination_ip == '504']


# In[5]:

def filter_504_and_cast_to_unix_ts(df):
    
    logs_pd_sorted_cleaned = df.drop(df[df.destination_ip  == '504'].index)

    #Sorting by visitor_ip inplace to bring all individual user hits together
    logs_pd_sorted_cleaned.sort(columns = ['visitor_ip'],axis = 0,inplace = True)

    #Converting date string to unix timestamp for convenience in sessionizing
    logs_pd_sorted_cleaned['timestamp'] = logs_pd_sorted_cleaned.timestamp.map                            (lambda x: time.mktime(datetime.datetime.strptime(x, "%Y-%m-%d %H:%M:%S").timetuple()))
    return logs_pd_sorted_cleaned


# In[13]:

def generate_sessions_sequence(df):
#     Grouping by visitor_ip
    logs_pd_sorted_cleaned_g = df.groupby('visitor_ip')

    #Creating session numbers for each individual user based on a session length of 15 min= 900seconds on basis on timestamp

    df['session_number'] = logs_pd_sorted_cleaned_g['timestamp']                                            .apply(lambda s: (s - s.shift(1) > 900)                                            .fillna(0).cumsum(skipna=False))
    return df


# In[7]:

def get_overall_average_session_time(df):
    
    avg = df.groupby(['visitor_ip','session_number'])['timestamp']                                    .apply(lambda x: max(x)-min(x)).reset_index()
    avg.columns = ['visitor_ip','session_number','average_time_per_session_per_user']
    
    #Calculating overall average session time (Comes out to be 1481 seconds ~ 25 mins)
    
    overall_average_session_time = avg.groupby('visitor_ip')                         .agg({'average_time_per_session_per_user' : np.mean})['average_time_per_session_per_user'].mean()
    return overall_average_session_time,avg



# In[16]:

def get_most_engaged_users(df):
    #Finding ip's of most engaged users by finding longest mean session times, 
    #this is done by sorting in descending after averaging

    copy_df = df.groupby('visitor_ip').agg({'average_time_per_session_per_user' : np.mean})['average_time_per_session_per_user']
    most_engaged_users = copy_df.reset_index().sort('average_time_per_session_per_user',ascending = False)
    return most_engaged_users
    


# In[8]:

def get_unique_urls_per_session(df):
    #unique url session numbers: 1. Apply groupby to get unique destination_ip per user per session
#                            2. Perform a count and again groupby to get count of unique urls visited per session

    unique_urls_per_session = df                                 .groupby(['visitor_ip','session_number','destination_ip']).count()                                 .reset_index().groupby(['visitor_ip','session_number'])                                 .count('destination_ip').reset_index()
    #Renaming and dropping extra columns
    unique_urls_per_session.columns = ['visitor_ip','session_number','unique_url','timestamp']
    unique_urls_per_session = unique_urls_per_session.drop('timestamp',axis=1)
    return unique_urls_per_session

    #Result is Number of unqiue url hits per session per user
    #IP Address does not guarantee distinct user therefore if we had user_id embedded withing the url then 
    #further distinctive analysis could have been done
    


# In[18]:

def df_to_csv(df,name):
    df.to_csv(name, sep=',', encoding='utf-8',headers = True)


# In[ ]:

if __name__ == '__main__':
    input_file = read_and_extract_fields('2015_07_22_mktplace_shop_web_log_sample.log')    
    splitted_input = input_file.map(get_log_stats)
    logs_pd = splitted_input.toDF().toPandas() #Converting to Pandas dataframe, takes 30 seconds approx for whole file.
    logs_pd.columns = ['timestamp','visitor_ip','destination_ip'] #Renaming columns of DF
#   gateway_timeout_response_entries = get_504_response_entries(logs_pd)
    filtered_df = filter_504_and_cast_to_unix_ts(logs_pd)
    sessions_sequence_df = generate_sessions_sequence(filtered_df)
    ovearall_mean,aggregated_df = get_overall_average_session_time(filtered_df)
    most_engaged_users = get_most_engaged_users(aggregated_df)
    unique_urls_per_session = get_unique_urls_per_session(sessions_sequence_df)
    df_to_csv(unique_urls_per_session,'unique_urls_per_session.csv')
    df_to_csv(most_engaged_users,'most_engaged_users.csv')
    print "Overall Average session time:",ovearall_mean, "seconds"
    print "Check directory for result csv's on most_engaged_users and unique_urls_per_session"


    


# In[ ]:



