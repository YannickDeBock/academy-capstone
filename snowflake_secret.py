# -*- coding: UTF-8 -*-
import boto3, json

def get_secret():
    secret_name = "snowflake/capstone/login"
    region_name = "eu-west-1"

    session = boto3.Session()
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