import boto3
import time
import json
import subprocess
from datetime import datetime, timedelta
from send_email import send_email

def lambda_handler(event, context):
    s3_file_list = []
    
    s3_client=boto3.client('s3')
    for object in s3_client.list_objects_v2(Bucket='midterm-yw', Prefix='data/de_midterm_raw')['Contents']:
        s3_file_list.append(object['Key'])
    # print('s3_file_list:', s3_file_list)
    
    # datestr = time.strftime("%Y-%m-%d")
    datestr = datetime.today().strftime("%Y-%m-%d")
    # print("datestr:",datestr)
    
    required_file_list = [f'data/de_midterm_raw/calendar_{datestr}.csv', f'data/de_midterm_raw/inventory_{datestr}.csv', f'data/de_midterm_raw/product_{datestr}.csv', f'data/de_midterm_raw/sales_{datestr}.csv', f'data/de_midterm_raw/store_{datestr}.csv']
    # print('required_file_list',required_file_list)
    
    # scan S3 bucket
    if set(required_file_list).issubset(set(s3_file_list)):
        s3_file_url = ['s3://' + 'midterm-yw' + a for a in s3_file_list]
        # print("s3_file_url",s3_file_url)
        table_name = [a[:] for a in s3_file_list] 
        print("table_name",table_name)
    
        # data = json.dumps({'conf':{a:b for a in table_name for b in s3_file_url}})
        # print("data",data)
        data_to_send = {
            'conf': {
                'data': {a:b for a,b in zip(table_name, s3_file_url)}
            }
        }
        
        data = json.dumps(data_to_send)
    # send signal to Airflow    
        endpoint= 'http://50.19.226.128:8080/api/v1/dags/midterm_dag/dagRuns'
        
        subprocess.run([
            'curl','-X','POST',
            endpoint,
            '-H', 
            'Content-Type: application/json',
            '--user',
            'airflow:airflow',
            '--data',
            data])
        print('File are send to Airflow')
    else:
        send_email()
