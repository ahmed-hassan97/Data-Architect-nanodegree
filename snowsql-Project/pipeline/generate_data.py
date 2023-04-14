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
stock1_bucket_name = 'material-price-stock1'
stock2_bucket_name = 'material-price-stock2'
Material = ["oil","wood","gas"]


def generate_material_data_stock1(day , month):
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

def generate_material_data_stock2(day , month):
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
        df_1 ,date = generate_material_data_stock1(day=i , month=3)
        df_2 ,date = generate_material_data_stock2(day=i , month=3)
        # Convert the DataFrame to a CSV file
        csv_buffer_stock1 = StringIO()
        csv_buffer_stock2 = StringIO()
        df_1.to_csv(csv_buffer_stock1, index=False)
        df_2.to_csv(csv_buffer_stock2, index=False)
        
        csv_buffer_stock1.seek(0)
        csv_buffer_stock2.seek(0)
        
        csv_object_key_stock1 = '{}.csv'.format(date)
        csv_object_key_stock2 = '{}.csv'.format(date)
       
        s3.put_object(Bucket=stock1_bucket_name, Key=csv_object_key_stock1, Body=csv_buffer_stock1.getvalue())
        s3.put_object(Bucket=stock2_bucket_name, Key=csv_object_key_stock2, Body=csv_buffer_stock2.getvalue())



if __name__ == "__main__":
    connect_aws()