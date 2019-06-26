import pandas as pd
import pymysql
import requests
from os import getcwd
from git import Repo
from shutil import copyfile
import argparse, ast, boto3, json, os, pyodbc, subprocess, sys, shutil

import os

def fetch_sql_file_from_git(git_location, sql_file):
    if (os.path.exists('Common-Queries')):
        dir_name = git_location.split('/')[-1].split('.')[0]
        print(dir_name)
        sql_file = sql_file.split("/")[-1]
        print(sql_file)
        print(type(sql_file))
        sql_file = sql_file.replace('"', '')
        sql_file = sql_file.replace(',', '')
        if 'day' in sql_file:
            dir_name = dir_name + "/consumer_metric_queries/daily/compute_sql/"  # Commenting for now
            print(dir_name)
        elif 'week' in sql_file:
            dir_name = dir_name + "/consumer_metric_queries/weekly/compute_sql/"
        elif 'month' in sql_file:
            dir_name = dir_name + "/consumer_metric_queries/monthly/compute_sql/"
        sql_file_location = '{}{}'.format(dir_name, sql_file)
        print(sql_file_location)
        if os.path.exists(sql_file_location) and 'day' in sql_file:
            shutil.copy(sql_file_location, 'C:/Users/vinay.m/PycharmProjects/SQL/Daily')
        elif os.path.exists(sql_file_location) and 'week' in sql_file:
            shutil.copy(sql_file_location, 'C:/Users/vinay.m/PycharmProjects/SQL/Weekly')
        elif os.path.exists(sql_file_location) and 'month' in sql_file:
            shutil.copy(sql_file_location, 'C:/Users/vinay.m/PycharmProjects/SQL/Monthly')
        else:
            with open('SQLnotfound1.txt', 'a') as fp:
                print(sql_file_location, file=fp)
    else:
        git_repo_download_command = "git clone --single-branch --branch master {}".format(git_location)
        subprocess.call(git_repo_download_command, shell=True)
        dir_name = git_location.split('/')[-1].split('.')[0]
        print(dir_name)
        sql_file = sql_file.split("/")[-1]
        print(sql_file)
        print(type(sql_file))
        sql_file = sql_file.replace('"', '')
        sql_file = sql_file.replace(',', '')
        if 'day' in sql_file:
            dir_name = dir_name + "/consumer_metric_queries/daily/data_sql/"  # Commenting for now
            print(dir_name)
        elif 'week' in sql_file:
            dir_name = dir_name + "/consumer_metric_queries/weekly/data_sql/"
        elif 'month' in sql_file:
            dir_name = dir_name + "/consumer_metric_queries/monthly/data_sql/"
        sql_file_location = '{}{}'.format(dir_name, sql_file)
        print(sql_file_location)
        if os.path.exists(sql_file_location) and 'day' in sql_file:
            shutil.copy(sql_file_location, 'C:/Users/vinay.m/PycharmProjects/SQL/Daily')
        elif os.path.exists(sql_file_location) and 'week' in sql_file:
            shutil.copy(sql_file_location, 'C:/Users/vinay.m/PycharmProjects/SQL/Weekly')
        elif os.path.exists(sql_file_location) and 'month' in sql_file:
            shutil.copy(sql_file_location, 'C:/Users/vinay.m/PycharmProjects/SQL/Monthly')
        else:
            with open('SQLnotfound2.txt', 'a') as fp:
                print(sql_file_location, file=fp)

def fetch_data_sql_file_from_git(git_location, sql_file):
    if (os.path.exists('Common-Queries')):
        dir_name = git_location.split('/')[-1].split('.')[0]
        print("RespositoryName =", dir_name)
        sql_file = sql_file.split("/")[-1]
        print("SQL_File_Name", sql_file)
        print(type(sql_file))
        sql_file = sql_file.replace('"', '')
        sql_file = sql_file.replace(',', '')
        if 'day' in sql_file:
            dir_name = dir_name + "/consumer_metric_queries/daily/data_sql/"  # Commenting for now
            print("COMPLETE DAY DIR NAME", dir_name)
        elif 'week' in sql_file:
            dir_name = dir_name + "/consumer_metric_queries/weekly/data_sql/"
            print("COMPLETE WEEK DIR NAME", dir_name)
        elif 'month' in sql_file:
            dir_name = dir_name + "/consumer_metric_queries/monthly/data_sql/"
            print("COMPLETE MONTH DIR NAME", dir_name)
        sql_file_location = '{}{}'.format(dir_name, sql_file)
        print("SQLFILE_LOCATION", sql_file_location)
        if os.path.exists(sql_file_location) and 'day' in sql_file:
            shutil.copy(sql_file_location, 'C:/Users/vinay.m/PycharmProjects/SQL/Data/Daily')
            print("Copied DAY SQL")
        elif os.path.exists(sql_file_location) and 'week' in sql_file:
            shutil.copy(sql_file_location, 'C:/Users/vinay.m/PycharmProjects/SQL/Data/Weekly')
            print("Copied WEEK SQL")
        elif os.path.exists(sql_file_location) and 'month' in sql_file:
            shutil.copy(sql_file_location, 'C:/Users/vinay.m/PycharmProjects/SQL/Data/Monthly')
            print("Copied MONTH SQL")
        else:
            with open('SQLnotdatafound1.txt', 'a') as fp:
                print(sql_file_location, file=fp)
    else:
        git_repo_download_command = "git clone --single-branch --branch master {}".format(git_location)
        subprocess.call(git_repo_download_command, shell=True)
        dir_name = git_location.split('/')[-1].split('.')[0]
        print("RespositoryName =",  dir_name)
        sql_file = sql_file.split("/")[-1]
        print("SQL_File_Name", sql_file)
        print(type(sql_file))
        sql_file = sql_file.replace('"', '')
        sql_file = sql_file.replace(',', '')
        if 'day' in sql_file:
            dir_name = dir_name + "/consumer_metric_queries/daily/data_sql/"  # Commenting for now
            print("COMPLETE DAY DIR NAME", dir_name)
        elif 'week' in sql_file:
            dir_name = dir_name + "/consumer_metric_queries/weekly/data_sql/"
            print("COMPLETE WEEK DIR NAME", dir_name)
        elif 'month' in sql_file:
            dir_name = dir_name + "/consumer_metric_queries/monthly/data_sql/"
            print("COMPLETE MONTH DIR NAME", dir_name)
        sql_file_location = '{}{}'.format(dir_name, sql_file)
        print("SQLFILE_LOCATION", sql_file_location)
        if os.path.exists(sql_file_location) and 'day' in sql_file:
            shutil.copy(sql_file_location, 'C:/Users/vinay.m/PycharmProjects/SQL/Data/Daily')
            print("Copied DAY SQL")
        elif os.path.exists(sql_file_location) and 'week' in sql_file:
            shutil.copy(sql_file_location, 'C:/Users/vinay.m/PycharmProjects/SQL/Data/Weekly')
            print("Copied WEEK SQL")
        elif os.path.exists(sql_file_location) and 'month' in sql_file:
            shutil.copy(sql_file_location, 'C:/Users/vinay.m/PycharmProjects/SQL/Data/Monthly')
            print("Copied MONTH SQL")
        else:
            with open('SQLnotdatafound2.txt', 'a') as fp:
                print(sql_file_location, file=fp)



fields = ['metric_compute_sql_location']
df = pd.read_csv("Duplicate_Metrics.csv", skipinitialspace=True, usecols=fields)
for i in df.metric_compute_sql_location:
    print(i)
    fetch_sql_file_from_git("git@github.move.com:DataEngineering/Common-Queries.git", i)


print("DONE FOR COMPUTE METRICS")

fields = ['metric_data_sql_location']
df = pd.read_csv("Duplicate_Metrics.csv", skipinitialspace=True, usecols=fields)
for i in df.metric_data_sql_location:
    print(i)
    fetch_data_sql_file_from_git("git@github.move.com:DataEngineering/Common-Queries.git", i)







