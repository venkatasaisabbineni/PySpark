from pyspark.sql import SparkSession
import boto3
import os

aws_access_key_id = "AKIA2D6ELNQSYKZ62EIU"
aws_secret_access_key = "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"
spark = SparkSession.builder \
    .appName("DataFrameSQL") \
    .master("local[*]") \
    .config("spark.driver.bindAddress", "127.0.0.1") \
    .config("spark jars","s3://aws-sam-cli-managed-default-samclisourcebucket-uhkdck5i59ka/hadoop-aws-3.2.2.jar") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.hadoop.fs.s3a.access.key", "AKIA2D6ELNQSYKZ62EIU") \
    .config("spark.hadoop.fs.s3a.secret.key", aws_secret_access_key) \
    .config("spark.hadoop.fs.s3a.aws.credentials.provider","org.apache.hadoop.fs.s3a.TemporaryAWSCredentialsProvider") \
    .getOrCreate()

s3_client = boto3.client("s3")
input_path = s3_client.get_object(Bucket="aws-sam-cli-managed-default-samclisourcebucket-uhkdck5i59ka", Key="persons.csv")
csv_content = input_path['Body'].read().decode('utf-8')
#c
lines_rdd = spark.sparkContext.parallelize(csv_content.split("\n"))
df = spark.read.csv(lines_rdd.map(lambda x: x), header=True)
df.show()
#my condition
df.createOrReplaceTempView("persons_table")
result = spark.sql("SELECT * FROM persons_table WHERE age > 25")
result.show()
result1 = result.coalesce(1)
#write to csv
result1.write.csv("/tmp/a/",header = True,mode="overwrite")
#take only csv files
files_in_dir = os.listdir("/tmp/a/")
csv_files = [file for file in files_in_dir if file.endswith('.csv')]
#upload to s3
s3_client.upload_file(os.path.join("/tmp/a/", csv_files[0]), "aws-sam-cli-managed-default-samclisourcebucket-uhkdck5i59ka", "output.csv")
spark.stop()