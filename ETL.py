from pyspark import SparkConf
from pyspark.sql import SparkSession
import configparser, os, boto3, pyspark.sql.functions as psf, json

config = configparser.ConfigParser()
config.read(os.path.expanduser("~/.aws/credentials"))
access_id = config.get('default', "aws_access_key_id") 
access_key = config.get('default', "aws_secret_access_key")

conf = SparkConf()
conf.set('spark.jars.packages', 'org.apache.hadoop:hadoop-aws:3.1.2,net.snowflake:spark-snowflake_2.12:2.9.0-spark_3.1,net.snowflake:snowflake-jdbc:3.13.3')
conf.set("spark.hadoop.fs.s3a.aws.credentials.provider","com.amazonaws.auth.DefaultAWSCredentialsProviderChain")
conf.set("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")

SNOWFLAKE_SOURCE_NAME = 'net.snowflake.spark.snowflake'
spark = SparkSession.builder.config(conf=conf).getOrCreate()

def get_secret():
    secret_name = "snowflake/capstone/login"
    region_name = "eu-west-1"

    session = boto3.session.Session()
    client = session.client(
        service_name='secretsmanager',
        region_name=region_name,
    )

    get_secret_value_response = client.get_secret_value(
        SecretId=secret_name
    )
    
    text_secret_data = get_secret_value_response['SecretString']
    secret = json.loads(text_secret_data)
    return secret


df = spark.read.json('s3a://dataminded-academy-capstone-resources/raw/open_aq/')
df = df.select('*','coordinates.*','date.*').drop('coordinates','date')
df = df.withColumn('local',psf.to_timestamp(psf.col('local')))
df = df.withColumn('utc',psf.to_timestamp(psf.col('utc')))

secret = get_secret()
sfOptions = {
  "sfURL" : secret['URL']+'.snowflakecomputing.com',
  "sfUser" : secret['USER_NAME'],
  "sfRole" : secret['ROLE'],
  "sfPassword" : secret['PASSWORD'],
  "sfDatabase" : secret['DATABASE'],
  "sfSchema" : "YANNICK",
  "sfWarehouse" : secret['WAREHOUSE']
}

SNOWFLAKE_SOURCE_NAME = 'net.snowflake.spark.snowflake'
df.write.format(SNOWFLAKE_SOURCE_NAME).options(**sfOptions).option("dbtable", "air_quality").mode('overwrite').save()
