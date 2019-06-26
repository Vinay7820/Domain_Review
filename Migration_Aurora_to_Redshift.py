#!/usr/bin/env python
# -*- coding: utf-8 -*-

'''
Created on: 04-17-2019
Summary: Script to load the data for the given
         month from aurora to Redshift
@author: vinay
'''

from sqlalchemy import create_engine
import pandas as pd
import argparse
import sys
import pymysql

# redshift_hostname = 'move-dataeng-redshift-ad-prod-090617-redshift-1xc94o00l3nyp.cyyvzspbwztx.us-west-2.redshift.amazonaws.com'
# redshift_user ='de_db_user'
# redshift_password = 'MoveDataEng2017'
# redshift_db = 'de_db'
# redshift_port = 5439



def redshift_db(df):
    '''
        This function is to connect to Redshift and insert aurora data into redshift staging table
        :return:  None
    '''
    connstr = 'postgresql://de_db_user:MoveDataEng2017@move-dataeng-redshift-ad-prod-090617-redshift-1xc94o00l3nyp.cyyvzspbwztx.us-west-2.redshift.amazonaws.com:5439/de_db'
    engine = create_engine(connstr)
    print("Connection to Redshift successful")
    print("Executing Query to insert the data")
    df.to_sql(name='staging_aurora_to_redshift_pure_leads', con=engine, index=False, schema='domain_review' , if_exists='append')
    print("Data inserted into redshift table successfully")
    #print("Checking data")
    #data_frame = pd.read_sql_query('SELECT * FROM domain_review.staging_aurora_to_redshift_pure_leads limit 10;', engine)
    #print(data_frame)



def aurora_db(query):
    '''
        This function is to connect to Aurora and fetch the data from aurora table
        :return:  dataframe with data
    '''
    host="move-dataeng-notification-s-notificationdbcluster-you48zm49vly.cluster-c86dwymt9wi3.us-west-2.rds.amazonaws.com"
    port=3306
    dbname="consumer"
    user="movedataengprod"
    password="dataeng_aurora_prod"
    conn = pymysql.connect(host, user=user,port=port,passwd=password, db=dbname)
    print("Connection to Aurora successful")
    print("Query =", query)
    print("Executing Query to fetch the data")
    df = pd.read_sql_query(query, con=conn)
    #print(df)
    return df


if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description='''This python fetch the data from Aurora and load into Redshift for the given month.''',
        formatter_class=argparse.RawTextHelpFormatter
    )
    parser.add_argument('-start_date', '--start_date', type=str,
                        help='(required) for every execution of this script.')
    args = vars(parser.parse_args())
    start_date = args['start_date']
    start_date = "'"+start_date+"'"
    arguments_count = len(sys.argv)

    query = """select L.redshift_metric_id ,redshift_metric_type ,C.* from consumer.aurora_to_redshift_lookup_pure_leads L
        JOIN(
        select A.metric_system_name, A.period_start_date,A.period_end_date,A.metric_value from consumer.metrics_fact A
        JOIN (select distinct metric_system_name from consumer.aurora_to_redshift_lookup_pure_leads) B
        on A.metric_system_name = B.metric_system_name 
        where A.is_lptd = 'n' and A.is_runrate = 'n' 
        and A.period_start_date={0})C
        on C.metric_system_name=L.metric_system_name""".format(start_date)

    if (arguments_count > 1):
        print("Connecting to Aurora DB")
        aurora_df = aurora_db(query)
        print("Aurora Result = ", aurora_df)
        print("Connecting to Redshift to dump the data")
        redshift_db(aurora_df)
    else:
        print("Argument count is not correct")
        print("Usage examples: python Migration_Aurora_to_Redshift.py --start_date '2019-07-01'")





