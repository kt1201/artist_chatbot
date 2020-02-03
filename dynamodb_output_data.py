import sys
import os
import boto3

from boto3.dynamodb.conditions import Key, Attr

def main():

    # boto3 연결
    try:
        dynamodb = boto3.resource('dynamodb', region_name='ap-northeast-2', endpoint_url='http://dynamodb.ap-northeast-2.amazonaws.com')
    except:
        logging.error('could not connect to dynamodb')
        sys.exit(1)

    table = dynamodb.Table('top_tracks')

    # primary key값을 알 때 Querying, 다른 값을 알 때 모든 값을 Scanning
    # 해당 artist_id인 것들 중 popularity가 80 이상인 것
    response = table.query(
        KeyConditionExpression=Key('artist_id').eq('0L8ExT028jH3ddEcZwqJJ5'),
        FilterExpression=Attr('popularity').gt(80)
    )
    print(response['Items'])
    print(len(response['Items']))



if __name__=='__main__':
    main()
