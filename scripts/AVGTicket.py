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
# scanFilter = Attr('orderID').eq('YK41BAJY')
scanFilter = Attr('orderID').exists()

session = boto3.Session(profile_name='tangotech')
dynamodb = session.resource('dynamodb')

ordersTable = dynamodb.Table(ordersTableName)

response = ordersTable.scan(FilterExpression=scanFilter)
orders = response['Items']

totalGross = sum(v['grossAmount'] for v in orders)
avg = totalGross / len(orders)

print('Gross Amount:', round(totalGross,2))
print('AVG Ticket:', round(avg, 2))