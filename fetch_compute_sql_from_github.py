import pandas as pd
import pymysql
import os
import requests
from os import getcwd
from git import Repo
from shutil import copyfile
import argparse, ast, boto3, json, os, pyodbc, subprocess, sys, shutil

from os.path import exists, join
from os import pathsep

def search_file(name, path):
    for root, dirs, files in os.walk(path):
        if name in files:
            return os.path.join(root, name)

if __name__ == '___main__':
    search_path = 'C:/Users/vinay.m/PycharmProjects/Learnings/Common-Queries/consumer_metric_queries/daily/compute_sql'
    fields = ['MSN']
    df = pd.read_csv("MSN_Fetch.csv", skipinitialspace=True, usecols=fields)
    for i in df.MSN:
        print(i)
        find_file = search_file(i,search_path)
        print(find_file)
        if find_file:
            print ("File found at %s" % find_file)
        else:
            print ("File not found")

