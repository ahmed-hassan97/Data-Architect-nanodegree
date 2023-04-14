import boto3
import pandas as pd
from datetime import datetime, timedelta
import random
from io import StringIO
import json
# set the folder path
# set the S3 credentials and bucket information
aws_access_key_id = 'AKIAW5AFKK2PBX6MCT4M'
aws_secret_access_key = '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'
csv_bucket_name = 'material-price-csv'
json_bucket_name = 'material-price-json'
Material = ["oil","wood","gas"]


def generate_material_data(day , month):
    # generate a list of timestamps with minutes
    start_time = datetime(2022,month, int(day+1), 0, 0, 0)
    end_time = datetime(2022, month, int(day+1), 23, 59, 59)
    time_diff = timedelta(minutes=1)
    timestamps = pd.date_range(start=start_time, end=end_time, freq=time_diff)

    # generate a list of numbers
    min_value = [random.randint(100.0, 120.0) for i in range(len(timestamps))]
    max_value = [random.randint(110.0, 130.0) for i in range(len(timestamps))]
    material = [random.choice(Material) for i in range(len(timestamps))]
    # create a dictionary of data
    data = {'timestamp': timestamps,'material': material,'min_value': min_value , 'max_value': max_value}
    date = start_time.date()
    date = str(date).replace('-','_')
    # create a DataFrame from the dictionary
    df = pd.DataFrame(data)


    return df , date


def connect_aws():

 # create an S3 client
    s3 = boto3.client('s3',
                    aws_access_key_id=aws_access_key_id,
                    aws_secret_access_key=aws_secret_access_key)
    
    for i in range(30):
        df ,date = generate_data(day=i , month=3)
        # Convert the DataFrame to a CSV file
        csv_buffer = StringIO()

        df.to_csv(csv_buffer, index=False)
        data_dict = df.to_dict(orient='index')
        csv_buffer.seek(0)
       
        csv_object_key = '{}.csv'.format(date)
       
        s3.put_object(Bucket=csv_bucket_name, Key=csv_object_key, Body=csv_buffer.getvalue())
      
