import json
import boto3
from boto3.dynamodb.conditions import Key, Attr

session = boto3.Session(profile_name='tangotech')

dynamodb = session.resource('dynamodb')
table = dynamodb.Table('customersDB-livetplug')
scanFilter = Attr('accountID').eq('26a83fe1-f868-4a68-9f68-8e2cf622b2bb') & Attr('classification').not_exists()

response = table.scan(
    FilterExpression=scanFilter
)
items = response['Items']

for item in items:
    table.update_item(
        Key={
            'accountID': item['accountID'],
            'customerID': item['customerID']
            },
        UpdateExpression='SET classification = :val1',
        ExpressionAttributeValues={
            ':val1': 'Revendedor'
            }
    )
    print(item['customerID'])