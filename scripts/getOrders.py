import json
import sys
import decimal
import requests
import boto3
from boto3.dynamodb.conditions import Key, Attr

def set_default(obj):
    if isinstance(obj, set):
        return list(obj)
    if isinstance(obj, decimal.Decimal):
        return float(obj)
    raise TypeError

ordersTableName = 'ordersDB-livetplug'
scanFilter = Attr('orderID').exists()

session = boto3.Session(profile_name='tangotech')
dynamodb = session.resource('dynamodb')

ordersTable = dynamodb.Table(ordersTableName)

response = ordersTable.scan(FilterExpression=scanFilter)
orders = response['Items']

output = []

for order in orders:
    for item in order['items']:
        if item['externalProductID'] == '12.01.01.001':
            status = len(item['status'])
            if item['status'][status-1]['description'] == 'BILLED' or item['status'][status-1]['description'] == 'SENT':
                output.append({
                    'orderID':order['orderID'], 
                    'externalProductID':item['externalProductID'],
                    'quantity':item['quantity']
                    })

if output:
    dataOutput = json.dumps(output, default=set_default)
    print(dataOutput)