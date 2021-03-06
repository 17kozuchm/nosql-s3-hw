import boto3
import csv
import codecs
key = 'AKIA37JNAJADYDLRN3E7'
code = 'qJxv/Q3NdUTbeBtYGDZiuaq6Y6sbSgBY1e1y5Pt1'


#get access to s3 resource previously created on web interface
s3 = boto3.resource('s3', aws_access_key_id = key, aws_secret_access_key = code)
bucket = s3.Bucket("nosql-hw")
#access dynamodb
dyndb = boto3.resource('dynamodb', region_name='us-east-2', aws_access_key_id=key,aws_secret_access_key = code)
try: 
    table = dyndb.create_table(
    TableName='DataTable',
    KeySchema=[
        {
            'AttributeName': 'PartitionKey',
            'KeyType': 'HASH'
        },
        {
        'AttributeName': 'RowKey',
        'KeyType': 'RANGE'
        }
    ],
    AttributeDefinitions=[
        {
        'AttributeName': 'PartitionKey',
        'AttributeType': 'S'
        },
        {
            'AttributeName': 'RowKey',
            'AttributeType': 'S'
        },
    ],
    ProvisionedThroughput={
        'ReadCapacityUnits': 5,
        'WriteCapacityUnits': 5
    }
    )
except:
    table = dyndb.Table("DataTable")
table.meta.client.get_waiter('table_exists').wait(TableName='DataTable')
#print(table.item_count)
#upload data to table
with open('test.csv', 'rb') as exp:
    csvf = csv.reader(codecs.iterdecode(exp, 'utf-8'), delimiter=',', quotechar='|')
    for item in csvf:
        #print(item)
        body = open(item[3]+'.csv', 'rb')
        s3.Object('nosql-hw', item[3]).put(Body=body )
        md = s3.Object('nosql-hw', item[3]).Acl().put(ACL='public-read')
        url = "https://s3-us-east-2.amazonaws.com/nosql-hw/"+item[3]
        data_item = {'PartitionKey': item[0], 'RowKey': item[1],'date' : item[2], 'name' : item[3], 'url':url}
        print(data_item)
        try:
            table.put_item(Item=data_item)
        except:
            print("item may already be there or another failure")
    
#Test data
response = table.get_item(
    Key={'PartitionKey': 'test1', 'RowKey': '1'}
)
item = response['Item']
print("ITEM")
print(item)
print("FULL RESPONSE")
print(response)