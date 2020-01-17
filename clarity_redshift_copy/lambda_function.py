import json
import boto3
import psycopg2
import os


def lambda_handler(event, context):
    try:
        key = ""
        s3 = boto3.resource('s3')
        bucket_name = "clarity-ua-redshift-staging"

        records = event["Records"][0]["body"]
        records = json.loads(records)
        # key = "2020/01/15/20/clarity_ua_s3_redshift_stream-1-2020-01-15-20-51-52-24abe2b3-bcf8-4993-bdcc-776db4a19d4a"
        key = records["Records"][0]["s3"]["object"]["key"]
        print(key)
        generate_query(key)

    except Exception as e:
        print(e)
        publish_sqs(key)


def generate_query(key):
    query = f"""copy user_analytics 
                from 's3://clarity-ua-redshift-staging/{key}' 
                iam_role 'arn:aws:iam::138547552813:role/redshift-dynamo-copy'
                format as json 'auto';"""

    # connect to redshift and execute the query
    copy_data(query)


def copy_data(query):
    dbname = os.environ['dbname']
    user = os.environ['user']
    host = os.environ['host']
    password = os.environ['password']
    port = os.environ['port']
    conn_string = f"dbname='{dbname}' user='{user}' host='{host}' password='{password}' port={port}"
    connection = psycopg2.connect(conn_string)
    # use DictCursor so we can access columns by their name
    cursor = connection.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    try:
        cursor.execute(query)
        connection.commit()
        return 0
    except Exception as e:
        print(e)
        connection.close()
    finally:
        if connection is not None:
            connection.close()


def publish_sqs(record):
    sqs = boto3.client('sqs')

    queue_url = 'https://sqs.us-east-1.amazonaws.com/138547552813/clarity_ua_redshift_copy_dlq'

    # Send message to SQS queue
    response = sqs.send_message(
        QueueUrl=queue_url,
        DelaySeconds=10,
        MessageAttributes={
            'Title': {
                'DataType': 'String',
                'StringValue': 'A ua redshift loading issue has occurred'
            },
        },
        MessageBody=(json.dumps(record))
    )

    print(response['MessageId'])
    message = f"Successfully Pushed Failed record {str(record)} to the SQS Queue"
    print(message)


if __name__ == "__main__":
    lambda_handler()