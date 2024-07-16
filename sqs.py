import boto3
sqs_client = boto3.client('sqs',region_name = 'us-east-1',aws_access_key_id = aws_access_key_id1,aws_secret_access_key = aws_secret_access_key1)
queue_url = "https://sqs.us-east-1.amazonaws.com/695659621413/bobbyqueue.fifo"
response = sqs_client.send_message(
    QueueUrl=queue_url,
    MessageBody=(
        'id,name,category,quantity,price'
        '1,iPhone,Electronics,10,899.99'
        '2,Macbook,Electronics,5,1299.99'
        '3,iPad,Electronics,15,499.99'
        '4,Samsung TV,Electronics,8,799.99'
        '5,LG TV,Electronics,10,699.99'
        'He'
    ),
    MessageGroupId = 'a',
    MessageDeduplicationId = 'a'
)
print(response['MessageId'])
