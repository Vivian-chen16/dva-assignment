#!/usr/bin/env python
# coding: utf-8

# # HW3 Q4 [10 pts]
# 
# 
# 
# ## Important Notices
# 
# <div class="alert alert-block alert-danger">
#     WARNING: Do <strong>NOT</strong> add any cells to this Jupyter Notebook, because that will crash the autograder. Additionally, Make sure to delete or comment out any code you add to this notebook for testing purposes prior to submitting the notebook to Gradescope. Failure to do so may crash the autograder. 
# </div>
# 
# 
# All instructions, code comments, etc. in this notebook **are part of the assignment instructions**. That is, if there is instructions about completing a task in this notebook, that task is not optional.  
# 
# 
# 
# <div class="alert alert-block alert-info">
#     You <strong>must</strong> implement the following functions in this notebook to receive credit.
# </div>
# 
# `user()`
# 
# `load_data()`
# 
# `exclude_no_pickuplocations()`
# 
# `exclude_no_tripdistance()`
# 
# `include_fare_range()`
# 
# `get_highest_tip()`
# 
# `get_total_toll()`
# 
# Each method will be auto-graded using different sets of parameters or data, to ensure that values are not hard-coded.  You may assume we will only use your code to work with data from NYC Taxi Trips during auto-grading. You do not need to write code for unreasonable scenarios.  
# 
# Since the overall correctness of your code will require multiple function to work together correctly (i.e., all methods are interdepedent), implementing only a subset of the functions likely will lead to a low score.
# 
# ### Helper functions
# 
# You should not use - and do not need - any helper functions; implement the required functions by completing the function skeletons below. All of the fuctions you implement should be self-contained; each function should carry all of the code it needs to execute on its expected input(s) within its body. 
# 
# ### Kernel Selection
# 
# Be sure that you select the PySpark kernel from the kernel options above, if it is not already selected. Do not use the Python3 kernel. The notebook should default to the PySpark kernel. 

# #### Pyspark Imports
# <span style="color:red">*Please don't modify the below cell*</span>

# In[1]:


import pyspark
from pyspark.sql import SQLContext
from pyspark.sql import *


# #### Define Spark Context
# <span style="color:red">*Please don't modify the below cell*</span>

# In[2]:


sc
sqlContext = SQLContext(sc)


# ### Student Section - Please compete all the functions below

# #### Function to return GT Username

# In[3]:


def user():
        """
        :return: string
        your GTUsername, NOT your 9-Digit GTId  
        """         
        return 'wchen678'


# #### Function to load data

# In[4]:


def load_data(gcp_storage_path):
    """
        :param gcp_storage_path: string (full gs path including file name e.g gs://bucket_name/data.csv) 
        :return: spark dataframe  
    """
    
    ################################################################
    # code to load yellow_tripdata_2019-01.csv data from your GCP storage bucket#
    #                                                              #        
    ################################################################
    df = spark.read.load(gcp_storage_path,format="csv", sep=",", inferSchema="true", header="true")
    
    return df


# #### Function to exclude trips that don't have a pickup location

# In[18]:


def exclude_no_pickuplocations(df):
    """
        :param nyc tax trips dataframe: spark dataframe 
        :return: spark dataframe  
    """
    
    ################################################################
    # code to exclude trips with no pickup locations               #
    # Note: Exclude nulls and zeros                                #        
    ################################################################
    
    return df.filter(df.pulocationid.isNotNull()).filter(df.pulocationid != 0)


# #### Function to exclude trips with no distance

# In[13]:


def exclude_no_tripdistance(df):
    """
        :param nyc tax trips dataframe: spark dataframe 
        :return: spark dataframe  
    """
    
    ################################################################
    # code to exclude trips with no trip distances                 #
    # Note: Exclude nulls and zeros                                #        
    ################################################################
    exclude_no_pickuplocations(df)
    df = df.withColumn("trip_distance", df.trip_distance.cast('decimal')).filter(df.trip_distance.isNotNull()).filter(df.trip_distance != 0.)
    
    return df


# #### Function to include fare amount between the range of 20 to 60 Dollars

# In[14]:


def include_fare_range(df):
    """
        :param nyc tax trips dataframe: spark dataframe 
        :return: spark dataframe  
    """
    
    ################################################################
    # code to include trips with only within the fare range of     #
    # 20 to 60 dollars (including 20 and 60 dollars)               #        
    ################################################################
    exclude_no_tripdistance(df)
    df = df.withColumn("fare_amount", df.fare_amount.cast('decimal')).filter((df.fare_amount >= 20.) & (df.fare_amount <= 60.))
    
    return df


# #### Function to get the highest tip amount

# In[111]:


from pyspark.sql.functions import *

def get_highest_tip(df):
    """
        :param nyc tax trips dataframe: spark dataframe 
        :return: decimal (rounded to 2 digits)  (NOTE: DON'T USE FLOAT)
    """
    
    ################################################################
    # code to get the highest tip amount                           #
    #                                                              #        
    ################################################################
    include_fare_range(df)
    df = df.withColumn("tip_amount", df.tip_amount.cast('decimal(10,2)'))
    max_tip = df.agg({"tip_amount": "max"}).collect()[0][0]

    
    return max_tip


# #### Function to get total toll amount

# In[115]:


def get_total_toll(df):
    """
        :param nyc tax trips dataframe: spark dataframe 
        :return: decimal (rounded to 2 digits)  (NOTE: DON'T USE FLOAT)
    """
    
    ################################################################
    # code to get total toll amount                                #
    #                                                              #        
    ################################################################
    include_fare_range(df)
    df = df.withColumn("tolls_amount", df.tolls_amount.cast('decimal(10,2)'))
    total_toll = df.agg({"tolls_amount": "sum"}).collect()[0][0]
    
    return total_toll


# ### Run above functions and print
# 
# #### Uncomment the cells below and test your implemented functions

# #### Load data from yellow_tripdata09-08-2021.csv

# In[5]:


gcp_storage_path = "gs://wchen678/yellow_tripdata09-08-2021.csv"
df = load_data(gcp_storage_path)
#df.printSchema()


# #### Print total numbers of rows in the dataframe

# In[6]:


#df.count()
#df.show()


# #### Print total number of rows in the dataframe after excluding trips with no pickup location

# In[19]:


df_no_pickup_locations = exclude_no_pickuplocations(df)
df_no_pickup_locations.count()


# #### Print total number of rows in the dataframe after exclude trips with no distance

# In[16]:


#df_no_trip_distance = exclude_no_tripdistance(df)
#df_no_trip_distance.count()


# #### Print total number of rows in the dataframe after including trips with fair amount between the range of 20 to 60 Dollars

# In[17]:


#df_include_fare_range = include_fare_range(df)
#df_include_fare_range.count()


# #### Print the highest tip amount

# In[112]:


#max_tip = get_highest_tip(df)
#print(max_tip)


# #### Print the total toll amount

# In[116]:


#total_toll = get_total_toll(df)
#print(total_toll)

