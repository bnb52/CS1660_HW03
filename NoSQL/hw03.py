import boto3
import sys

s3 = boto3.resource('s3',
	aws_access_key_id='AKIAREP2XV6KQDDXX52Y',
	aws_secret_access_key='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'
)

try:
	s3.create_bucket(Bucket='bryce-behr-hw3-bucket', CreateBucketConfiguration={
		'LocationConstraint': 'us-west-2'})
except Exception as e:
	print ()

bucket = s3.Bucket("bryce-behr-hw3-bucket")

bucket.Acl().put(ACL='public-read')

body = open('C:\\Users\\behrb\\OneDrive\\Desktop\\workspace\\cs1660\\hw03db\\datafiles\\exp1.csv', 'rb')

o = s3.Object('bryce-behr-hw3-bucket', 'test').put(Body=body )

s3.Object('bryce-behr-hw3-bucket', 'test').Acl().put(ACL='public-read')

dyndb = boto3.resource('dynamodb',
	region_name='us-west-2',
	aws_access_key_id='AKIAREP2XV6KQDDXX52Y',
	aws_secret_access_key='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'
)

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
except Exception as e:
	#print (e)
	#if there is an exception, the table may already exist. if so...
	table = dyndb.Table("DataTable")

table.meta.client.get_waiter('table_exists').wait(TableName='DataTable')

import csv

with open('C:\\Users\\behrb\\OneDrive\\Desktop\\workspace\\cs1660\\hw03db\\experiments.csv', 'rt') as csvfile:
	csvf = csv.reader(csvfile, delimiter=',', quotechar='|')
	for item in csvf:
		#print(item)
		try:
			body = open('C:\\Users\\behrb\\OneDrive\\Desktop\\workspace\\cs1660\\hw03db\\datafiles\\'+item[4], 'rb')
		except:
			print()
		s3.Object('bryce-behr-hw3-bucket', item[4]).put(Body=body )
		md = s3.Object('bryce-behr-hw3-bucket', item[4]).Acl().put(ACL='public-read')

		url = " https://s3-us-west-2.amazonaws.com/bryce-behr-hw3/"+item[4]
		metadata_item = {'PartitionKey': item[0], 'RowKey': item[4], 'temp': item[1], 'conductivity': item[2], 'concentration': item[3], 'url':url}
		try:
			table.put_item(Item=metadata_item)
		except:
			print("item may already be there or another failure")

response = table.get_item(
	Key={
		'PartitionKey': str(sys.argv[1]),
		'RowKey': str(sys.argv[2])
	}
)

item = response['Item']
print(item)