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
customersTableName = 'customersDB-livetplug'
# scanFilter = Attr('orderID').eq('20IQ404U')
scanFilter = Attr('customer').exists()

session = boto3.Session(profile_name='tangotech')
dynamodb = session.resource('dynamodb')

ordersTable = dynamodb.Table(ordersTableName)

response = ordersTable.scan(FilterExpression=scanFilter)
orders = response['Items']

if orders:
    for order in orders:
        # if not 'customer' in order:
        print('Updating order', order['orderID'])
        customerFilter = Attr('customerID').eq(order['customerID'])

        customersTable = dynamodb.Table(customersTableName)
        response = customersTable.scan(FilterExpression=customerFilter)
        customers = response['Items']

        if customers:
            for customer in customers:
                response = ordersTable.update_item(
                    Key={'accountID': order['accountID'],
                            'orderID': order['orderID']
                    },
                    UpdateExpression="set customer = :customer",
                    ExpressionAttributeValues={
                        ':customer': customer
                    },
                    ReturnValues="UPDATED_NEW"
                )
        else:
            print('Customer not found')