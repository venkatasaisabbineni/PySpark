import boto3
sqs_client = boto3.client('sqs',region_name = 'us-east-1',aws_access_key_id = aws_access_key_id1,aws_secret_access_key = aws_secret_access_key1)
queue_url = "https://sqs.us-east-1.amazonaws.com/695659621413/bobbyqueue.fifo"
response = sqs_client.receive_message(
    QueueUrl=queue_url,
    MaxNumberOfMessages=1,
    VisibilityTimeout=10,
    WaitTimeSeconds=5,
    #ReceiveRequestAttemptId='string'
)
message = response['Messages'][0]
receipt_handle = message['ReceiptHandle']

# Delete received message from queue
sqs_client.delete_message(
    QueueUrl=queue_url,
    ReceiptHandle=receipt_handle
)
body = message['Body']
print('Received and deleted message: ',body)
