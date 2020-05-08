
import boto3
import re
import time
from botocore.config import Config

class Execution():

    '''It helps you to execute data from AWS Athena'''

    def __init__(self, session, database, bucket, path, region):

        self.database = database
        self.bucket = bucket
        self.path = path
        self.client = session.client('athena', region_name=region, 
                    config=Config(proxies={"https": "******",
                    "http": "******"}))

    def athena_query(self, sql_query):
        print(self.client)
        
        response = self.client.start_query_execution(
                    QueryString=sql_query,
                    QueryExecutionContext={
                        'Database': self.database
                    },
                    ResultConfiguration={
                        'OutputLocation': 's3://' + self.bucket + '/' + self.path
                    }
                )
        self.execution_id = response['QueryExecutionId']
        return

    def athena_to_s3(self, max_execution = 300):

        state = 'RUNNING'
        while (max_execution > 0 and state in ['RUNNING']):
            max_execution = max_execution - 1
            response = self.client.get_query_execution(QueryExecutionId = self.execution_id)
            print(response['QueryExecution']['Status'])

            if 'QueryExecution' in response and \
                    'Status' in response['QueryExecution'] and \
                    'State' in response['QueryExecution']['Status']:
                state = response['QueryExecution']['Status']['State']
                if state == 'FAILED':
                    print(response['QueryExecution']['Status'])
                    return False
                elif state == 'SUCCEEDED':
                    s3_path = response['QueryExecution']['ResultConfiguration']['OutputLocation']
                    filename = re.findall(r'.*\/(.*)', s3_path)[0]
                    return filename

            time.sleep(1)
        
        return False