import json
import boto3
import sys
from boto3.dynamodb.conditions import Key, Attr
import decimal

def set_default(obj):
    if isinstance(obj, set):
        return list(obj)
    if isinstance(obj, decimal.Decimal):
        return float(obj)
    raise TypeError

session = boto3.Session(profile_name='tangotech')

dynamodb = session.resource('dynamodb')
table = dynamodb.Table('pricingSchemaDB-devtplug')

accountID = str(sys.argv[1])
documentType = str(sys.argv[2])

obligatoryFields = {}
output = {}

response = table.query(
    KeyConditionExpression=Key('accountID').eq(accountID) & Key('documentType').eq(documentType)
    )
items = response['Items']

for item in items:
    for schema in item['pricingSchema']:
        for accessSequence in sorted(schema['details']['accessSequence']):
            for sequence in schema['details']['accessSequence'][accessSequence]:
                
                sequence = str(sequence).replace('=#','',1)
                sequence = str(sequence).replace('>#','',1)
                sequence = str(sequence).replace('≥#','',1)
                sequence = str(sequence).replace('<#','',1)
                sequence = str(sequence).replace('≤#','',1)

                obligatoryFields.update({sequence:'none'})

output.update({'accountID':accountID})    
output.update({'documentType':documentType})
output.update({'obligatoryData':obligatoryFields})

dataOutput = json.dumps(output, default=set_default)
print(dataOutput)