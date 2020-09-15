import json
import boto3

session = boto3.Session(profile_name='tangotech')

dynamodb = session.resource('dynamodb')
table = dynamodb.Table('pricingValuesDB-livetplug')

counter = 0

with open('boxter-TVAL.json') as json_file:
    data = json.load(json_file)
    for p in data:
        counter += 1
        print(counter)
        table.put_item(
            Item=p
        )