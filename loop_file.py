import pandas as pd
import os
start_date = '20190307'
end_date = '20190331'
column_no = 12
daterange = pd.date_range(start_date, end_date)
for single_date in daterange:
    start_date = single_date.strftime("%Y%m%d")
    print(start_date)
    script_call = os.system('python 666666.py --start_date {0} --column_no {1}'.format(start_date, column_no))
    print(script_call)
    column_no += 1



# def decor(func1):
#     var = func1()
#     print("Error =", var)
#
#
# @decor
# def func1():
#     a = 5
#     b = 0
#     try:
#         r = a/b
#     except Exception as e:
#         return e

# import boto3
# from move_dl_common_api.athena_util import AthenaUtil
#
# client = boto3.client('athena')
#
# sql = """
# with vw_data
# as
# (
#   select  b.fulldate start_date
#          ,b.fulldate end_date
#          ,a.apps_type
#          ,a.experience_type
#          ,a.move_device_type
#          ,a.move_device_type_detail
#          ,a.property_status
#          ,a.property_status_sub
#          ,a.page_type_group
#          ,a.site_section
#          ,a.product_type_category
#          ,sum(a.pageview_count) pageview_count
#     from biz_data_product_event_v2.rdc_visitor_summary_daily a join dimensions.date_dim b
# on a.date_mst = b.fulldate
#    where a.event_date between '20190101' and '20190101'
#      and b.cy_datenum between cast('20190101' as integer) and cast('20190101' as integer)
#    group by
#           b.fulldate
#          ,a.apps_type
#          ,a.experience_type
#          ,a.move_device_type
#          ,a.move_device_type_detail
#          ,a.property_status
#          ,a.property_status_sub
#          ,a.page_type_group
#          ,a.site_section
#          ,a.product_type_category
# )
# select sum(case when  (apps_type = 'android instant apps' and experience_type = 'android instant apps')
# and  (property_status = 'for_rent')  then pageview_count end) from vw_data
# """
#
# def execute_on_athena_boto3(sql):
#     s3_staging_folder = "s3://move-dataeng-edw-historicaldata-dev/domain"
#     util = AthenaUtil(s3_staging_folder= s3_staging_folder)
#     QueryId = util.start_query_execution(sql_query = sql , s3_output_folder = s3_staging_folder)
#     result = util.get_results(QueryId)
#     print(result)
#     if result['ResultSet']['Rows'][0]['Data'][0]:
#         metric_value = result['ResultSet']['Rows'][0]['Data'][0]['VarCharValue']
#     else:
#         metric_value = "NULL"
#     print(metric_value)
#     #writetoexcel(row, metric_value)
#
# execute_on_athena_boto3(sql)