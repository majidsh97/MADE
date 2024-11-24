# %% [markdown]
# 

# %%
import pandas as pd
import sqlalchemy as sql
import seaborn as sns
import matplotlib.pyplot as plt
import umap
import sklearn as sk
import kagglehub
import os
import plotly.express as px
import plotly.graph_objects as go
import sqlalchemy as sql

# %% [markdown]
# ### Licence
# 
# I used Two datasets about origin citizenships of internashional studetns in canada and usa.
# 
# Both The us and canadian international students datasets license is CC0: Public Domain
# 
# - No Copyright: anyone can use them for any purpose.
# 
# - Free Use: Users can copy, modify, distribute, and perform the work, even for commercial purposes, without any restrictions.
# 
# - Global Applicability: CC0 is intended to apply globally, although the specifics may vary based on local copyright laws.
# 
# - No Attribution Required: it is not legally required when using CC0 works.
# 
# 
#  
# 

# %% [markdown]
# ## Data pipeline
# 
# The whole pipeline is in python and is structured as follows.
# 
# Getting the data -> Processing the data to two dataframes (meta and usage data) -> Transform data -> Store data
# 
# 

# %% [markdown]
# 
# ### Getting the data
# 
# The pipeline starts with a GET request to the provided url. I used pre-writen kaggle function to download CSV files.
# 

# %%

data_path= '/home/cip/ce/ix05ogym/Majid/MADE/data/'
urls = [
        "syedabdulshameer/international-students-in-canada",
        "webdevbadger/international-student-demographics",
        'justin2028/unemployment-in-america-per-us-state'
        ]

for url in urls:
    path = kagglehub.dataset_download(url)
    os.system(f'cp -r {path}/* {data_path}')
    print("Path to dataset files:", path)
    os.listdir(path)
    
    


# %% [markdown]
# ### Load Data

# %%
csv_names= os.listdir(data_path)
df ={}
for name in csv_names:
    name = name.split('.')
    extention = name[1]
    name = name[0]
    if extention != 'csv':
        continue
    
    print(name)
    df[name] = pd.read_csv(data_path+name+'.csv')
    #display(df[name])
    

Internation_students_canada = df['Internation_students_Canada']
Internation_students_us =df['origin']

# %% [markdown]
# ### visualise data

# %%
Internation_students_canada.head()

# %%
Internation_students_us.head()

# %% [markdown]
# 
# ### Processing and Transform the data
# 
# - it is important to check is there is any NaN for null field in the datasets. 
# - unify the column names of two datasets
# - Then we unify the datatypes of two datasets to make them prepare for merging. 
# - To merge two dataset we have to calculate all the international students no matter what their study level are. for this we use groupby and sum of the students in all the studying level.
# - Then I merged two datasets based on origin citizenship of international students.
# 

# %%
print(Internation_students_canada.isnull().any().any(),
Internation_students_canada.isna().any().any())


# %%
print(Internation_students_us.isnull().any().any(),
Internation_students_us.isna().any().any())

# %%
Internation_students_us =Internation_students_us.groupby(['origin','year']).agg({'students': 'sum'}).reset_index()#.plot(kind='bar', figsize=(12, 8), width=0.8, stacked=True,legend=False,)
Internation_students_us['year'] = Internation_students_us['year'].str.split('/').str[0]
Internation_students_us.head()

# %%
Internation_students_us = Internation_students_us.pivot(index='origin', columns='year', values='students').fillna(0)
Internation_students_us.head()

# %%
Internation_students_canada.rename(columns={'Country of Citizenship':'origin'}, inplace=True)
#Internation_students_canada

# %%
merged_table = pd.merge(Internation_students_us, Internation_students_canada, on='origin', how='inner', suffixes=('', '_Canada'))
merged_table.iloc[:, 1:] = merged_table.iloc[:, 1:].astype('float')
merged_table = merged_table[['origin',
                             '2015','2016', '2017', '2018', '2019', '2020', '2021', '2022',
                             '2015_Canada','2016_Canada', '2017_Canada', '2018_Canada', '2019_Canada','2020_Canada', '2021_Canada', '2022_Canada']]
merged_table.head()


# %% [markdown]
# ### Extract data into sqllite

# %%
engine = sql.create_engine('sqlite:///./data/international_students_north_america.db')
merged_table.to_sql('international_students_north_america', engine, if_exists='replace', index=False)


