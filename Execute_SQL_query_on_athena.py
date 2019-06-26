#!/usr/bin/env python
# -*- coding: utf-8 -*-

'''
Created on: 07-02-2019
Summary: Script to fetch the SQL queries (compute + data) from a .txt file and execute on athena, resultant columns are pushed to csv.
@author: vinay
'''
from move_dl_common_api.athena_util import AthenaUtil
from pyathena import connect
import pandas as pd

aws_region_name = 'us-west-2'
#s3_bucket = 'aws-athena-query-results-057425096214-us-west-2'
#s3_key = 'Unsaved/Abtest_data'
temp_location = 's3://move-dataeng-temp-dev/sql_refractor/'
result = pd.DataFrame()


with open('Input_SQL_Redshift.txt', 'r') as f:   ##### SQL.txt should hold the queries to be executed on the athena seperated by ";"
    s = f.read()
    d = s.split(';')

athena_df = pd.DataFrame()
util = AthenaUtil(s3_staging_folder=temp_location)

for _ in d:
    try:
        result  = util.execute_query(sql_query = _)
        temp = util.get_pandas_frame(result)
        print(temp)
        athena_df = pd.concat([athena_df, temp], ignore_index=True)

    except Exception as e:
        print("Exception")
        print("Not Executed --------- ", _)
        print(e)

#athena_df.columns = ['metric_id', 'start_date', 'end_date', 'fy_monthdimkey', 'metric_value']
athena_df.to_csv('ATHENA_SQL_OUTPUT.csv', index=False)
