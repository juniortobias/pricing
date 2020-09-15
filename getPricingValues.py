import ast
import json
import boto3
import sys
import decimal
import operator
import re
from datetime import datetime
from collections import Counter
from boto3.dynamodb.conditions import Key, Attr

def set_default(obj):
    if isinstance(obj, set):
        return list(obj)
    if isinstance(obj, decimal.Decimal):
        return float(obj)
    raise TypeError

session = boto3.Session(profile_name='tangotech')

dynamodb = session.resource('dynamodb')
pricingSchemaTable = dynamodb.Table('pricingSchemaDB-livetplug')
pricingValuesTable = dynamodb.Table('pricingValuesDB-livetplug')

##### INI - INPUT DATA #####
inputFile = './input.json'

with open(inputFile, 'r') as jsonFile:
    data = json.load(jsonFile)
##### END - INPUT DATA #####

obligatoryItemData = data['obligatoryData']
accountID = data['accountID']
documentType = data['documentType']


response = pricingSchemaTable.query(
    KeyConditionExpression=Key('accountID').eq(accountID) & Key('documentType').eq(documentType) 
    )
schemaItems = response['Items']

output = {}
dataOutput = {}
itemPricingValues = {}
suggestedPrice = 0

### Schema
for schemaItem in schemaItems:
    
    sortedSchemas = schemaItem['pricingSchema']
    sortedSchemas = sorted(sortedSchemas, key=lambda item: item.get('position'))

    ### Condition
    for schema in sortedSchemas:
        match = False

        condition = schema['condition']
        position = int(schema['position'])
        description = schema['description']
        conditionType = schema['conditionType']
        direction = schema['direction']
        
        if 'netProfitExclusion' in schema:
            netProfitExclusion = schema['netProfitExclusion']
        
        validFrom = datetime.strptime(schema['validFrom'],'%Y-%m-%dT%H:%M:%S.%fZ')
        validTo = datetime.strptime(schema['validTo'],'%Y-%m-%dT%H:%M:%S.%fZ')
        appliedFor = schema['appliedFor']
        
        netProfitAppliedFor = ''
        if 'netProfitAppliedFor' in schema:
            netProfitAppliedFor = schema['netProfitAppliedFor']

        if validFrom <= datetime.now() and validTo >= datetime.now():

            ### Sequence
            sortedAccessSequence = sorted(schema['details']['accessSequence'])

            for sequence in sortedAccessSequence:

                if match == False:

                    accessSequence = schema['details']['accessSequence'][sequence]
                    
                    ### Define logical operators in a separeted array
                    logicalOperatorList = {}
                    for field in accessSequence:
                        logicalOperatorList.update({field[2:]:field[:1]})

                    ### Define fields to be sorted
                    filterFields = []
                    for key in logicalOperatorList.keys():
                        filterFields.append(key)

                    ### Define according to logical operator the sorting type (asc/dec)
                    if str(accessSequence).find('≤') > 0 or str(accessSequence).find('<') > 0:
                        reverseSort = False
                    else:
                        reverseSort = True

                    itemResponse = pricingValuesTable.scan(FilterExpression=Attr('accountID').eq(accountID) & Attr('pricingValueKey').eq(condition) & Attr('sequence').eq(sequence))
                    itemPricingValues = itemResponse['Items']

                    sortedItems = sorted(itemPricingValues, key=operator.itemgetter(*filterFields), reverse=reverseSort)

                    ### Price values table read
                    if itemPricingValues:
                        for item in sortedItems:
                            for key, value in item.items():

                                ### Check if there is field match between payload and price values
                                if key in logicalOperatorList.keys() and item['sequence'] == sequence:
                                    if   logicalOperatorList[key] == '=':
                                        if obligatoryItemData[key] == item[key]:
                                            match = True
                                        else:
                                            match = False
                                            break
                                    elif logicalOperatorList[key] == '>':
                                        if int(obligatoryItemData[key]) > int(item[key]):
                                            match = True
                                        else:
                                            match = False
                                            break
                                    elif logicalOperatorList[key] == '≥':
                                        if int(obligatoryItemData[key]) >= int(item[key]):
                                            match = True
                                        else:
                                            match = False
                                            break
                                    elif logicalOperatorList[key] == '<':
                                        if int(obligatoryItemData[key]) < int(item[key]):
                                            match = True
                                        else:
                                            match = False
                                            break
                                    elif logicalOperatorList[key] == '≤':
                                        if int(obligatoryItemData[key]) <= int(item[key]):
                                            match = True
                                        else:
                                            match = False
                                            break

                            if match == True:
                                beforeSign = ''
                                afterSign = ''
                                value = 0
                                rate = 0
                                summarize = 0

                                ### Sum conditions to applied rates
                                for af in appliedFor:
                                    if af != 'initial' and output:
                                        summarize += output[af]['amount']
                                    else:
                                        summarize += item['value']

                                ### Amount
                                if direction == 'increase' and conditionType == 'amount':
                                    suggestedPrice += item['value']
                                    
                                    value = item['value']
                                    beforeSign = '+R$ '
                                elif direction == 'decrease' and conditionType == 'amount':
                                    suggestedPrice -= item['value']
                                    
                                    value = item['value'] * -1
                                    beforeSign = '-R$ '

                                ## Percentage
                                elif direction == 'increase' and conditionType == 'percentage':
                                    rate = item['value']
                                    value = suggestedPrice
                                    amount = summarize * (item['value'] / 100)
                                    suggestedPrice += amount
                                    value = suggestedPrice - value

                                    beforeSign = '+'
                                    afterSign = '%'
                                elif direction == 'decrease' and conditionType == 'percentage':
                                    rate = item['value']
                                    value = suggestedPrice
                                    amount = summarize * (item['value'] / 100)
                                    suggestedPrice -= amount
                                    value = suggestedPrice - value

                                    beforeSign = '-'
                                    afterSign = '%'
                                
                                output.update({condition:{'netProfitExclusion':netProfitExclusion,'description':description,'sequence':sequence, 'rate':rate, 'amount':value, 'currentSuggestedPrice':suggestedPrice, 'netProfitAppliedFor': netProfitAppliedFor}})
                                break
                            else:
                                output.update({condition:'unmatched'})
                    else:
                        output.update({condition:'unmatched'})                      
        else:
            output.update({condition:'outdated'})                   

if output:
    dataOutput.update({'pricing':output})
    dataOutput.update({'suggestedPrice':suggestedPrice})

    ### Calc Net Profit
    amount = 0

    for item in dataOutput['pricing']:
        if dataOutput['pricing'][item]['rate'] == 0 and not dataOutput['pricing'][item]['netProfitExclusion']:
            amount += dataOutput['pricing'][item]['amount']
        elif dataOutput['pricing'][item]['rate'] != 0 and not dataOutput['pricing'][item]['netProfitExclusion'] and dataOutput['pricing'][item]['netProfitAppliedFor']:
            netProfitAppliedFor = dataOutput['pricing'][item]['netProfitAppliedFor']
            if netProfitAppliedFor != 'suggestedPrice':
                amount += dataOutput['pricing'][netProfitAppliedFor]['amount']
            else:
                amount += suggestedPrice * dataOutput['pricing'][item]['rate'] / 100
        
        print(item, ' - ', amount)
    
    if amount > 0:
        print('Amount:', amount)
        netProfitValue = suggestedPrice - amount
        netProfitRate = netProfitValue / suggestedPrice * 100

        dataOutput.update({'netProfitValue':netProfitValue})
        dataOutput.update({'netProfitRate':netProfitRate})

    print(json.dumps(dataOutput, default=set_default))