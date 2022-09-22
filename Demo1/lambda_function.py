import json
import boto3
from boto3.dynamodb.conditions import Key

client = boto3.resource('dynamodb')
table = client.Table('Demo1Table')


def lambda_handler(event, context):
    #functions
    print(event)
    pushBatch(event) #pushes everything from the JSON into the table
    #deleteContents(event) #deletes all entries in the table
    #addRecord(Item) #pushes a single record into the table
    #queryStuff("accountID","123456") #pulls a single item from the table
    #deleteItem("accountID","123456") #deletes a single item based on primary key
    #updateRecord("123456", "Greg", "travel", "american airlines","150.00") #updates a record given primary key and what is being updated
    return(event)

    #return {
    #    'statusCode': 200,
    #    'body': json.dumps('Hello from Lambda!'),
    #    'body': queryStuff("accountID","123456"),
    #    'body': event[0]
    #}

def queryStuff(key, val): #pushes everything from the JSON into the table
    response = table.query(KeyConditionExpression = Key(key).eq(val))
    print(response['Items'])
    return response['Items']
    
def addRecord(Item): #pushes a single record into the table
    response = table.put_item(Item = Item) 

def pushBatch(Item):
    for stuff in Item:
        addRecord(stuff)
        
def deleteItem(accountID): 
    response = table.delete_item(Key={'accountID': accountID})

def deleteContents(Item): #deletes all entries in the table
    scan = table.scan()
    with table.batch_writer() as batch:
        for each in scan['Items']:
            batch.delete_item(
                Key={
                    'accountID': each['accountID']
                }
            )   
            
            
def updateRecord(accountID, name, transaction_type, vendor, amount):
    deleteItem(accountID)
    Item = {
        'accountID' : accountID,
        'name' : name,
        'transaction_type' : transaction_type,
        'vendor' : vendor,
        'amount' : amount
    }
    addRecord(Item)

def deleteTable():
    table.delete()
