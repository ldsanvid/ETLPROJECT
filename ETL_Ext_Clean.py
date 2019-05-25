#!/usr/bin/env python
# coding: utf-8

# In[42]:


import pandas as pd
import numpy as np


# In[43]:


file1 = '../Desktop/deaths5.csv'
deaths = pd.read_csv(file1)
deaths = deaths.rename(columns={'Population':'Population1'})
deaths.head()


# In[45]:


deseases = deaths['Cause of Death'].unique()
deseases = list(deseases)
deseases
# 


# In[46]:


file2= '../Desktop/states_alls.csv'
education = pd.read_csv(file2)
education.head()
# from https://www.kaggle.com/noriuk/us-education-datasets-unification-project#states_all.csv


# In[47]:


#Instruction expenditure per state
education15 = education.loc[(education.YEAR==2015)]
education15 = education15[['STATE','YEAR','ENROLL','TOTAL_REVENUE',
                          'INSTRUCTION_EXPENDITURE']]
education15 = education15.dropna()
education15 = education15.reset_index()
education15 = education15[['STATE','YEAR','INSTRUCTION_EXPENDITURE']]
education15 = education15.rename(columns={'STATE':'comodin1'})

education15.head()


# In[48]:


file3 = '../Desktop/transport.csv'
transportation = pd.read_csv(file3)
transportation = transportation.rename(columns={'State':'comodin3'})
transportation.head()
# from https://www.bts.gov/content/transportation-revenues-collected-state-and-local-governments


# In[49]:


#Drug-related deaths per state, including state's population
file4 = "../Desktop/drugpoison.csv"

Drugs = pd.read_csv(file4)

drugs2016 = Drugs.loc[(Drugs["Year"]==2015)]
drugs2016 = drugs2016[drugs2016.State != 'United States']
drugs2016 = drugs2016.reset_index()
drugs2016 = drugs2016[['State', 'Deaths', 'Population']]
drugs2016 = drugs2016.sort_values("State", ascending = True)
drugs2016 = drugs2016.rename(columns={'State':'comodin2'})
drugs2016.head()


# In[54]:


#Concatenation of all the databases extracted.

master = pd.concat([Cancer, education15, transportation, drugs2016], axis=1, join='inner')

master.head()


# In[55]:


master = master[['State','Deaths', "Population",'INSTRUCTION_EXPENDITURE',
                         ]]
master.head()


# In[76]:


import pandas as pd
from sqlalchemy import create_engine


# In[77]:


rds_connection_string = "postgres:Sweater1693@localhost:5432/etl_db_final"
engine = create_engine(f'postgresql://{rds_connection_string}')


# In[78]:


engine.table_names()


# In[95]:


master['death % of population'] = master['Deaths']/master['Population']*100
master.head()


# In[96]:


master.rename(columns={'death % of population':'deathPopulation'}, inplace=True)


# In[97]:


master.head()


# In[99]:


master.to_sql(name='etl_db_final',con=engine, if_exists='append', index=False)


# In[100]:


engine.table_names()


# In[101]:


pd.read_sql_query('SELECT * FROM public.etl_db_final', con=engine).head()


# In[102]:


deaths = deaths[deaths.State != 'United States']

deaths16 = deaths.loc[(deaths.Year==2015) &
           (deaths.Locality=='All') &
           (deaths['Age Range']== '0-84') &
           (deaths.Benchmark == '2010 Fixed') &
           (deaths['Cause of Death'] == 'Unintentional Injury')]

Cancer = deaths16[['State','Observed Deaths','Expected Deaths','Population1']]
Cancer = Cancer.sort_values('State', ascending = True)
Cancer = Cancer.reset_index()
Cancer = Cancer[['State','Observed Deaths','Expected Deaths','Population1']]
Cancer = Cancer.replace('District of\nColumbia','District of Columbia')



Cancer.head()


# In[103]:


master2 = pd.concat([Cancer, education15], axis=1, join='inner')

master2.head()


# In[104]:


master2 = master2[['State','Observed Deaths', "Expected Deaths",'INSTRUCTION_EXPENDITURE'
                         ]]
master2.head()


# In[105]:


master2.to_sql(name='observed_expected',con=engine, if_exists='append', index=False)

