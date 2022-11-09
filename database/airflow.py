'''
This script helps to upload data to S3
Customize variables:
* source_file_path
* target_bucket
* target_key
and make sure that your AWS credentials in config.cfg are up to date.
'''


import boto3
import configparser

config = configparser.ConfigParser()
config.read('../../config.cfg')
aws = config['AWS']

KEY = aws['KEY']
SECRET = aws['SECRET']

s3 = boto3.client(
    's3',
    region_name='eu-west-1',
    aws_access_key_id=KEY,
    aws_secret_access_key=SECRET
)



source_file_path = "C:\Users\Genet Shanko\A_startUp\A_Startup_Data-_Warehouse-\Dataset\DATA_20181030_d10_0830_0900 (1).csv"
target_bucket = "Data engineering"
target_key = "input_data/GlobalLandTemperaturesByCountry.csv"


s3.upload_file(source_file_path, target_bucket, target_key)