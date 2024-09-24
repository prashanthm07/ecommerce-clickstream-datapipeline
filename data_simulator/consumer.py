import json
import pandas as pd
import os
import boto3
import time
from dotenv import load_dotenv

from confluent_kafka import Consumer, KafkaError
from confluent_kafka.serialization import SerializationContext, MessageField, StringDeserializer
from confluent_kafka.schema_registry import SchemaRegistryClient


from azure.storage.blob import BlobServiceClient, BlobClient, ContainerClient

KAFKA_TOPIC = "click-stream"
kafka_consumer = Consumer({'bootstrap.servers': 'localhost:9092', 'group.id':'0',
        'auto.offset.reset': 'earliest'})

kafka_consumer.subscribe([KAFKA_TOPIC])

load_dotenv()

# aws 
AWS_ACCESS_KEY_ID = os.getenv('AWS_ACCESS_KEY_ID')
AWS_SECRET_ACCESS_KEY = os.getenv('AWS_SECRET_ACCESS_KEY')
REGION_NAME = os.getenv('REGION_NAME')
SESSION_TOKEN = os.getenv('SESSION_TOKEN')
BUCKET_NAME = os.getenv('BUCKET_NAME')
S3_KEY_PREFIX = os.getenv('S3_KEY_PREFIX')

# az
CONNECTION_STRING = os.getenv('CONNECTION_STRING')
CONTAINER_NAME = os.getenv('CONTAINER_NAME')

BATCH_COUNT = 1000

s3_client = boto3.client('s3', aws_access_key_id=AWS_ACCESS_KEY_ID, aws_secret_access_key=AWS_SECRET_ACCESS_KEY, region_name=REGION_NAME, aws_session_token=SESSION_TOKEN)

blob_service_client = BlobServiceClient.from_connection_string(CONNECTION_STRING)
container_client = blob_service_client.get_container_client(CONTAINER_NAME)

def upload_to_s3(data, filename):
    s3_client.put_object(Body=data, Bucket=BUCKET_NAME, Key=S3_KEY_PREFIX+filename)

def upload_to_azure(data, filename):
    print(f"Uploading {filename} to Azure Blob Storage")
    blob_client = container_client.get_blob_client(filename)
    blob_client.upload_blob(data)

def process_batch(batch):
    # Implement your logic to process the batch of messages
    df = pd.DataFrame(json.loads(x) for x in batch)
    #upload_to_azure(df.to_parquet(index=False), f"click-stream-data-{int(time.time())}.parquet")
    # upload_to_s3(df.to_parquet(index=False), f"{int(time.time())}-batch.parquet")


try:
    batch = []
    while True:
        msg = kafka_consumer.poll(timeout=1.0)
        if msg is None:
            continue
        if msg.error():
            if msg.error().code() == KafkaError._PARTITION_EOF:
                continue
            else:
                print(msg.error())
                break
        print('Received message: {}'.format(msg.value().decode('utf-8')))
        batch.append(msg.value().decode('utf-8'))
        print(len(batch))
        if len(batch) == BATCH_COUNT:
            # Process the batch of messages
            process_batch(batch)
            print(f"Processed a batch of {BATCH_COUNT} messages")
            batch = []
except KeyboardInterrupt:
    pass
finally:
    kafka_consumer.close()