import json
import boto3

session = boto3.Session(profile_name='tangotech')

dynamodb = session.resource('dynamodb')
table = dynamodb.Table('productsDB-devtplug')

response = table.scan()
items = response['Items']

for item in items:
    table.update_item(
        Key={
            'productID': item['productID']
            },
        UpdateExpression='SET productType = :val1',
        ExpressionAttributeValues={
            ':val1': 'OLEO LUBRIFICANTE'
            }
    )
    print(item['productID'])