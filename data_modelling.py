import sys
import os
import logging
import pymysql
import boto3
import time
import math

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

    athena = boto3.client('athena')

    # artist avg
    query = """
        SELECT
         artist_id,
         AVG(danceability) AS danceability,
         AVG(energy) AS energy,
         AVG(loudness) AS loudness,
         AVG(speechiness) AS speechiness,
         AVG(acousticness) AS acousticness,
         AVG(instrumentalness) AS instrumentalness
        FROM
         top_tracks t1
        JOIN
         audio_features t2 ON t2.id = t1.id AND CAST(t1.dt AS DATE) = DATE('2020-01-26') AND CAST(t2.dt AS DATE) = DATE('2020-01-26')
        GROUP BY t1.artist_id
    """

    r = query_athena(query, athena)
    results = get_query_result(r['QueryExecutionId'], athena)
    artists = process_data(results)

    # max min
    query = """
        SELECT
         MIN(danceability) AS danceability_min,
         MAX(danceability) AS danceability_max,
         MIN(energy) AS energy_min,
         MAX(energy) AS energy_max,
         MIN(loudness) AS loudness_min,
         MAX(loudness) AS loudness_max,
         MIN(speechiness) AS speechiness_min,
         MAX(speechiness) AS speechiness_max,
         ROUND(MIN(acousticness),4) AS acousticness_min,
         MAX(acousticness) AS acousticness_max,
         MIN(instrumentalness) AS instrumentalness_min,
         MAX(instrumentalness) AS instrumentalness_max
        FROM
         audio_features
    """
    r = query_athena(query, athena)
    results = get_query_result(r['QueryExecutionId'], athena)
    avgs = process_data(results)[0]

    # euclidean distance
    metrics = ['danceability', 'energy', 'loudness', 'speechiness', 'acousticness', 'instrumentalness']

    for i in artists:
        for j in artists:
            dist = 0
            for k in metrics:
                x = float(i[k])
                x_norm = normalize(x, float(avgs[k+'_min']), float(avgs[k+'_max']))
                y = float(j[k])
                y_norm = normalize(y, float(avgs[k+'_min']), float(avgs[k+'_max']))
                dist += (x_norm-y_norm)**2

            dist = math.sqrt(dist) ## euclidean distance

            data = {
                'artist_id': i['artist_id'],
                'y_artist': j['artist_id'],
                'distance': dist
            }

            insert_row(cursor, data, 'related_artists')


    conn.commit()
    cursor.close()

# normalization
def normalize(x, x_min, x_max):

    normalized = (x-x_min) / (x_max-x_min)

    return normalized

# athena query하는 function
def query_athena(query, athena):
    response = athena.start_query_execution(
        QueryString=query,
        QueryExecutionContext={
            'Database': 'default'
        },
        ResultConfiguration={
            'OutputLocation': "s3://athena-kt-tables/repair/",
            'EncryptionConfiguration': {
                'EncryptionOption': 'SSE_S3'
            }
        }
    )

    return response

# query 결과를 받음
def get_query_result(query_id, athena):

    response = athena.get_query_execution(
        QueryExecutionId=str(query_id) # query_athena의 response에 query_id 들어있음
    )

    while response['QueryExecution']['Status']['State'] != 'SUCCEEDED':
        if response['QueryExecution']['Status']['State'] == 'FAILED':
            logging.error('QUERY FAILED')
            break
        time.sleep(5)
        response = athena.get_query_execution(
            QueryExecutionId=str(query_id)
        )

    response = athena.get_query_results(
        QueryExecutionId=str(query_id),
        MaxResults=1000 # athena의 MaxResults는 1000
    )

    return response

# 아테나로 받은 데이터들을 리스트화
def process_data(results):
    # column 이름들 가져옴
    columns = [col['Label'] for col in results['ResultSet']['ResultSetMetadata']['ColumnInfo']]

    # 데이터들 listed_results에 넣기
    listed_results = []
    for res in results['ResultSet']['Rows'][1:]:
        values = []
        for field in res['Data']:
            try:
                values.append(list(field.values())[0])
            except:
                values.append(list(' '))
        listed_results.append(dict(zip(columns, values)))

    return listed_results


def insert_row(cursor, data, table):

    placeholders = ', '.join(['%s'] * len(data))
    columns = ', '.join(data.keys())
    key_placeholders = ', '.join(['{0}=%s'.format(k) for k in data.keys()])
    sql = "INSERT INTO %s ( %s ) VALUES ( %s ) ON DUPLICATE KEY UPDATE %s" % (table, columns, placeholders, key_placeholders)
    cursor.execute(sql, list(data.values())*2)





if __name__=='__main__':
    main()
