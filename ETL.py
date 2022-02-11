# -*- coding: UTF-8 -*-
from pyspark import SparkConf
from pyspark.sql import SparkSession
from snowflake_secret import get_secret
import pyspark.sql.functions as psf, os

SNOWFLAKE_SOURCE_NAME = 'net.snowflake.spark.snowflake'
#Initalize spark
conf = SparkConf()
conf.set('spark.jars.packages', 'org.apache.hadoop:hadoop-aws:3.1.2,net.snowflake:spark-snowflake_2.12:2.9.0-spark_3.1,net.snowflake:snowflake-jdbc:3.13.3')
conf.set("spark.hadoop.fs.s3a.aws.credentials.provider","com.amazonaws.auth.DefaultAWSCredentialsProviderChain")
conf.set("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
spark = SparkSession.builder.config(conf=conf).getOrCreate() 

def read_data():
    df = spark.read.json('s3a://dataminded-academy-capstone-resources/raw/open_aq/')
    df = df.select('*','coordinates.*','date.*').drop('coordinates','date')
    df = df.withColumn('local',psf.to_timestamp(psf.col('local')))
    df = df.withColumn('utc',psf.to_timestamp(psf.col('utc')))
    return df

def write_data(df, format = SNOWFLAKE_SOURCE_NAME):
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

    df.write.format(SNOWFLAKE_SOURCE_NAME).options(**sfOptions).option("dbtable", "air_quality").mode('overwrite').save()


if __name__ == "__main__":
    #Read data
    df = read_data()

    #Write data
    write_data(df)