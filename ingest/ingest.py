# -*- coding: UTF-8 -*-
import openaq, datetime as dt, json, boto3
api = openaq.OpenAQ()
s3 = boto3.client('s3')

params = ['pm10','pm25','co','co2']
for city in ['Antwerpen','Amsterdam','London','Madrid']:
    resp = api.measurements(city=city, parameter=params, date_from=dt.date.today()-dt.timedelta(days=7), date_to=dt.date.today(), df=False)[1]
    s3.put_object(
        Body=json.dumps(resp),
        Bucket= "dataminded-academy-capstone-resources",
        Key="Yannick/ingest/data_"+city+".json"
    )