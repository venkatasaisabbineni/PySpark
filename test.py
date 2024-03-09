from pyspark.sql import SparkSession
import boto3
import os
import io

aws_access_key_id = "AKIA2D6ELNQSYKZ62EIU"
aws_secret_access_key = "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"
spark = SparkSession.builder \
    .appName("DataFrameSQL") \
    .master("local[*]") \
    .config("spark.driver.bindAddress", "127.0.0.1") \
    .config("spark.hadoop.fs.s3a.access.key", "AKIA2D6ELNQSYKZ62EIU") \
    .config("spark.hadoop.fs.s3a.secret.key", aws_secret_access_key) \
    .config("spark.hadoop.fs.s3a.aws.credentials.provider") \
    .getOrCreate()

s3_client = boto3.client("s3")
input_path = s3_client.get_object(Bucket="aws-sam-cli-managed-default-samclisourcebucket-uhkdck5i59ka", Key="persons.csv")
csv_content = input_path['Body'].read().decode('utf-8')
input_path = f"s3://aws-sam-cli-managed-default-samclisourcebucket-uhkdck5i59ka/persons.csv"
print("This is input path for bucket", input_path)
output_path = "s3://etlcustomerbobby/persons_greater_than_25.csv"

df = spark.read.option('header','true').csv(input_path)
df.createOrReplaceTempView("persons_table")
print("Result before spark.sql")

result = spark.sql("SELECT * FROM persons_table WHERE age > 25")
print("Result after spark.sql")
result.show()
print("Result before result.write.csv")
result.write.csv(output_path,header = True)
print("Result before result.write.csv")
spark.stop()