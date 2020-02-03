import sys
import requests
import base64
import json
import logging
import pymysql
import csv


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

    headers = get_headers(client_id, client_secret)

    ## Spotify Search API

    # artist라는 csv 리스트 생성
    artists = []
    with open('artist_list.csv') as f:
        raw = csv.reader(f)
        for row in raw:
            artists.append(row[0])

    # for문을 통하여 artist search
    for a in artists:
        params = {
            "q": a,
            "type": "artist",
            "limit": "1"
        }

        r = requests.get("https://api.spotify.com/v1/search", params=params, headers=headers)

        raw = json.loads(r.text)

        # 해당 artist를 가져와서 업데이트
        artist = {}
        try:
            artist_raw = raw['artists']['items'][0]
            if artist_raw['name'] == params['q']:

                artist.update(
                    {
                        'id': artist_raw['id'],
                        'name': artist_raw['name'],
                        'followers': artist_raw['followers']['total'],
                        'popularity': artist_raw['popularity'],
                        'url': artist_raw['external_urls']['spotify'],
                        'image_url': artist_raw['images'][0]['url']
                    }
                )
                insert_row(cursor, artist, 'artists')
        except:
            logging.error('NO ITEMS FROM SEARCH API')
            continue

    conn.commit()
    sys.exit(0)


    try:
        r = requests.get("https://api.spotify.com/v1/search", params=params, headers=headers)

    except:
        logging.error(r.text)
        sys.exit(1)


    r = requests.get("https://api.spotify.com/v1/search", params=params, headers=headers)

    if r.status_code != 200:
        logging.error(r.text)

        if r.status_code == 429:

            retry_after = json.loads(r.headers)['Retry-After']
            time.sleep(int(retry_after))

            r = requests.get("https://api.spotify.com/v1/search", params=params, headers=headers)

        ## access_token expired
        elif r.status_code == 401:

            headers = get_headers(client_id, client_secret)
            r = requests.get("https://api.spotify.com/v1/search", params=params, headers=headers)

        else:
            sys.exit(1)


    # Get BTS' Albums

    r = requests.get("https://api.spotify.com/v1/artists/3Nrfpe0tUJi4K4DXYWgMUX/albums", headers=headers)

    raw = json.loads(r.text)

    total = raw['total']
    offset = raw['offset']
    limit = raw['limit']
    next = raw['next']

    albums = []
    albums.extend(raw['items'])

    ## 난 100개만 뽑아 오겠다
    while next:

        r = requests.get(raw['next'], headers=headers)
        raw = json.loads(r.text)
        next = raw['next']
        print(next)

        albums.extend(raw['items'])
        count = len(albums)

    print(len(albums))



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


def insert_row(cursor, data, table):

    placeholders = ', '.join(['%s'] * len(data))
    columns = ', '.join(data.keys())
    key_placeholders = ', '.join(['{0}=%s'.format(k) for k in data.keys()])
    sql = "INSERT INTO %s ( %s ) VALUES ( %s ) ON DUPLICATE KEY UPDATE %s" % (table, columns, placeholders, key_placeholders)
    cursor.execute(sql, list(data.values())*2)




if __name__=='__main__':
    main()
