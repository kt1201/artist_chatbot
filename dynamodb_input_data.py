import sys
import os
import boto3
import requests
import base64
import json
import logging
import pymysql

# spotify id, pw
client_id = ""
client_secret = ""

host = "ktkim.ca3ahvepujnu.ap-northeast-2.rds.amazonaws.com"
port = 3306
username = ""
database = "production"
password = ""


def main():

    # boto3 연결
    try:
        dynamodb = boto3.resource('dynamodb', region_name='ap-northeast-2', endpoint_url='http://dynamodb.ap-northeast-2.amazonaws.com')
    except:
        logging.error('could not connect to dynamodb')
        sys.exit(1)

    # pymysql 연결
    try:
        conn = pymysql.connect(host, user=username, passwd=password, db=database, port=port, use_unicode=True, charset='utf8')
        cursor = conn.cursor()
    except:
        logging.error("could not connect to rds")
        sys.exit(1)


    headers = get_headers(client_id, client_secret)

    table = dynamodb.Table('top_tracks') # 테이블 define

    cursor.execute('SELECT id FROM artists') # cursor를 이용하여 DB에 sql문 전

    countries = ['US', 'CA']
    for country in countries:
        for (artist_id, ) in cursor.fetchall(): # artists 테이블을 for문으로 회전

            # spotify에서 요구하는 parameter 지정
            URL = "https://api.spotify.com/v1/artists/{}/top-tracks".format(artist_id)
            params = {
                'country': 'US'
            }

            # spotify에서 요구하는 parameter 입력
            r = requests.get(URL, params=params, headers=headers)

            # json으로 읽어오는 기능 raw 생성
            raw = json.loads(r.text)

            # track 하나씩 읽어오기
            # 한 artist에 대해서 artist_id 값으로 나눠서 data 값을 저장
            for track in raw['tracks']:

                data = {
                    'artist_id': artist_id,
                    'country': country
                }

                data.update(track)

                # DynamoDB에 읽어온 data 집어넣기
                table.put_item(
                    Item=data
                )




# key값을 가져오는 function
def get_headers(client_id, client_secret):

    endpoint = "https://accounts.spotify.com/api/token"
    encoded = base64.b64encode("{}:{}".format(client_id, client_secret).encode('utf-8')).decode('ascii')

    headers = {
        "Authorization": "Basic {}".format(encoded)
    }

    payload = {
        "grant_type": "client_credentials"
    }

    r = requests.post(endpoint, data=payload, headers=headers)

    access_token = json.loads(r.text)['access_token']

    headers = {
        "Authorization": "Bearer {}".format(access_token)
    }

    return headers


if __name__=='__main__':
    main()
