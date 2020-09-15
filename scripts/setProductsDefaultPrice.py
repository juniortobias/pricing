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

URL = 'https://hv8bysjrpf.execute-api.us-east-1.amazonaws.com/staging/pricingValuesFunction-devtplug'
HEADERS = {'x-api-key':'cXO6eIsOrz7Wg3Da2PjS84UzwsovIlKC6FVgvgAc'}

dataInput = {
    #BOXTER
    # "accountID": "ca5c091c-889f-4804-8284-e0e9db251c93",

    #FIT
    "accountID": "26a83fe1-f868-4a68-9f68-8e2cf622b2bb",
    "documentType": "B2B2B",
    "obligatoryData": {
        "header-customerRegion": "SP",
        "header-supplierRegion": "SP",
        "header-customerClassification": "Revendedor",
        "header-freightType": "CIF",
        "header-paymentCondition": "28",
        "item-externalProductID": "none",
        "item-ncm": "none"
    }
}

# BOXTER
# scanFilter = Attr('accountID').eq('ca5c091c-889f-4804-8284-e0e9db251c93') & Attr('externalProductID').exists()
# scanFilter = Attr('accountID').eq('ca5c091c-889f-4804-8284-e0e9db251c93') & Attr('externalProductID').eq('12.01.01.001')

# FIT
scanFilter = Attr('accountID').eq('26a83fe1-f868-4a68-9f68-8e2cf622b2bb') & Attr('externalProductID').exists()
# scanFilter = Attr('accountID').eq('26a83fe1-f868-4a68-9f68-8e2cf622b2bb') & Attr('externalProductID').eq('12.01.01.001')

session = boto3.Session(profile_name='tangotech')
dynamodb = session.resource('dynamodb')
productsTable = dynamodb.Table('productsDB-livetplug')

response = productsTable.scan(FilterExpression=scanFilter)
products = response['Items']

if products:
    for product in products:
        if product['externalProductID']:
            dataInput['obligatoryData']['item-externalProductID'] = product['externalProductID']  
        
        if product['ncm']:
            dataInput['obligatoryData']['item-ncm'] = product['ncm']      
        
        r = requests.post(URL,data=json.dumps(dataInput),headers=HEADERS)
        data = r.json()
        print('')
        print('-----------------')
        print('Product:', product['productID'])
        print(data)

        if 'P#TVAL' not in data:
            continue

        p = 0
        v = 0
        m = 0
        i = 0

        for pricing in data:
            if pricing[0] == 'P':
                if data[pricing]:
                    p += float(data[pricing])
            elif pricing[0] == 'V':
                if data[pricing]:
                    v += float(data[pricing])
                    v -= 1
            elif pricing[0] == 'M':
                if data[pricing]:
                    m += float(data[pricing])
                    m -= 1
            elif pricing[0] == 'I':
                if data[pricing]:
                    i += float(data[pricing])
                    i -= 1

        #FIT
        suggestedPrice = round(decimal.Decimal( p * (v+1) * (m+1) * (i+1) ),2)
        print('Preço:',p)
        print('Variáveis:',v+1)
        print('Margem:',m+1)
        print('Imposto:',i+1)
        
        
        #BOXTER
        # suggestedPrice = round(decimal.Decimal( p * (m+1) * (i+1) ),2)
        # print('Preço:',p)
        # print('Margem:',m+1)
        # print('Imposto:',i+1)
        
        print(suggestedPrice)
        response = productsTable.update_item(
            Key={'accountID': product['accountID'],
                 'productID': product['productID']
            },
            UpdateExpression="set defaultPrice = :price",
            ExpressionAttributeValues={
                ':price': suggestedPrice
            },
            ReturnValues="UPDATED_NEW"
        )