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

scanFilter = Attr('accountID').eq('ca5c091c-889f-4804-8284-e0e9db251c93') & Attr('customerID').exists()

session = boto3.Session(profile_name='tangotech')
dynamodb = session.resource('dynamodb')
customersTable = dynamodb.Table('customersDB-livetplug')

response = customersTable.scan(FilterExpression=scanFilter)
customers = response['Items']

if customers:
    for customer in customers:     

        changed = False
        if type(customer['fiscalCompanyID']) != str:
            print('String - ', 'String - ', customer['name'], customer['fiscalCompanyID'])
            customer['fiscalCompanyID'] = str(customer['fiscalCompanyID'])
            changed = True
        
        if len(str(customer['fiscalCompanyID'])) == 13:
            print('13 - ', customer['name'], customer['fiscalCompanyID'])
            customer['fiscalCompanyID'] = '0' + str(customer['fiscalCompanyID'])
            changed = True
        elif len(str(customer['fiscalCompanyID'])) == 12:
            print('12 - ', customer['name'], customer['fiscalCompanyID'])
            customer['fiscalCompanyID'] = '00' + str(customer['fiscalCompanyID'])
            changed = True
        elif len(str(customer['fiscalCompanyID'])) == 11:
            print('11 - ',customer['name'], customer['fiscalCompanyID'])
            customer['fiscalCompanyID'] = '000' + str(customer['fiscalCompanyID'])
            changed = True
        elif len(str(customer['fiscalCompanyID'])) == 10:
            print('10 - ',customer['name'], customer['fiscalCompanyID'])
            customer['fiscalCompanyID'] = '0000' + str(customer['fiscalCompanyID'])
            changed = True
        elif len(str(customer['fiscalCompanyID'])) == 9:
            print('09 - ',customer['name'], customer['fiscalCompanyID'])
            customer['fiscalCompanyID'] = '00000' + str(customer['fiscalCompanyID'])
            changed = True
        elif len(str(customer['fiscalCompanyID'])) == 8:
            print('08 - ',customer['name'], customer['fiscalCompanyID'])
            customer['fiscalCompanyID'] = '000000' + str(customer['fiscalCompanyID'])
            changed = True
        elif len(str(customer['fiscalCompanyID'])) == 7:
            print('07 - ',customer['name'], customer['fiscalCompanyID'])
            customer['fiscalCompanyID'] = '0000000' + str(customer['fiscalCompanyID'])
            changed = True

        if changed:
            response = customersTable.update_item(
                Key={'accountID': customer['accountID'],
                    'customerID': customer['customerID']
                },
                UpdateExpression="set fiscalCompanyID = :fiscalCompanyID",
                ExpressionAttributeValues={
                    ':fiscalCompanyID': customer['fiscalCompanyID']
                },
                ReturnValues="UPDATED_NEW"
            )