import sys
import os
import logging
import boto3
import requests
import base64
import json
import pymysql
from datetime import datetime
import pandas as pd
import jsonpath

client_id = ""
client_secret = ""

host = "ktkim.ca3ahvepujnu.ap-northeast-2.rds.amazonaws.com"
port = 3306
username = ""
database = "production"
password = ""

def main():

    try:
        conn = pymysql.connect(host, user=username, passwd=password, db=database, port=port, use_unicode=True, charset='utf8')
        cursor = conn.cursor()
    except:
        logging.error("could not connect to rds")
        sys.exit(1)

    # RDS - artists 데이터 가져오고
    cursor.execute("SELECT * FROM artists")

    # artists로 colnames 생성
    colnames = [d[0] for d in cursor.description]

    # artists.parquet형식으로 생성
    artists = [dict(zip(colnames, row)) for row in cursor.fetchall()] # colnames와 row를 dict형태로 만들어서 row마다 artists로 넣어줌
    artists = pd.DataFrame(artists)
    artists.to_parquet('artists.parquet', engine='pyarrow', compression='snappy')

    dt = datetime.utcnow().strftime("%Y-%m-%d")

    # S3에 저장
    s3 = boto3.resource('s3')
    object = s3.Object('spotify-artists-kt', 'artists/dt={}/artists.parquet'.format(dt))
    data = open('artists.parquet', 'rb')
    object.put(Body=data)



if __name__=='__main__':
    main()
