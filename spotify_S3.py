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

    # headers define 해서 spotify에서 token을 가져옴
    headers = get_headers(client_id, client_secret)

    # RDS - 아티스트 ID를 가져오고
    cursor.execute("SELECT id FROM artists")

    # top_track_keys define
    top_track_keys = {
        "id": "id",
        "name": "name",
        "popularity": "popularity",
        "external_url": "external_urls.spotify"
    }

    ## Top Tracks Spotify 가져오기
    top_tracks = [] # top_tracks dictionary 생성

    # 해당 artist마다 Top Track을 가져오기
    for (id, ) in cursor.fetchall():

        URL = "https://api.spotify.com/v1/artists/{}/top-tracks".format(id)
        params = {
            'country' : 'US'
        }
        r = requests.get(URL, params=params, headers=headers)
        raw = json.loads(r.text)

        # track 하나하나마다 dictionary 형태로 주어지게 됨
        for i in raw['tracks']:
            top_track = {} # top_track dictionary 생성
            # top_track_keys의 path의 데이터들을 가져옴
            for k, v in top_track_keys.items():
                top_track.update({k: jsonpath.jsonpath(i, v)}) # i dictionary의 v value값을 가져와라.("id": "id")
                top_track.update({'artist_id': id}) # mapping을 위해 추가
                top_tracks.append(top_track) # top_tracks에 append

    # top_tracks로 track_ids 생성
    track_ids = [i['id'][0] for i in top_tracks]

    # top-tracks.parquet형식으로 생성
    top_tracks = pd.DataFrame(top_tracks)
    top_tracks.to_parquet('top-tracks.parquet', engine='pyarrow', compression='snappy')

    dt = datetime.utcnow().strftime("%Y-%m-%d")

    # S3에 저장
    s3 = boto3.resource('s3')
    object = s3.Object('spotify-artists-kt', 'top-tracks/dt={}/top-tracks.parquet'.format(dt))
    data = open('top-tracks.parquet', 'rb')
    object.put(Body=data)


    ## audio_features Spotify 가져오기
    tracks_batch = [track_ids[i: i+100] for i in range(0, len(track_ids), 100)]

    # audio_features spotify batch 형식으로 가져오기
    audio_features = []
    for i  in tracks_batch:

        ids = ','.join(i)
        URL = "https://api.spotify.com/v1/audio-features/?ids={}".format(ids)

        r = requests.get(URL, headers=headers)
        raw = json.loads(r.text)

        audio_features.extend(raw['audio_features'])

    # audio_features.parquet형식으로 저장
    audio_features = pd.DataFrame(audio_features)
    audio_features.to_parquet('audio-features.parquet', engine='pyarrow', compression='snappy')

    # S3 저장
    s3 = boto3.resource('s3')
    object = s3.Object('spotify-artists-kt', 'audio-features/dt={}/audio-features.parquet'.format(dt))
    data = open('audio-features.parquet', 'rb')
    object.put(Body=data)


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
