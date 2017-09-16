#!/usr/bin/env python3
import boto3
import uuid
import json
import urllib.parse
import random
import datetime

s3_resource = boto3.resource('s3')
s3_client = boto3.client('s3')


class LandingZoneProcessor:

    def __init__(self):
        self.queue = boto3.resource('sqs').get_queue_by_name(QueueName='sdavids-bucket-create-events')

    def read_s3_messages(self):
        """Reads S3 insertion messages from SQS, continues to read until no messages are remaining on the queue"""
        while True:
            messages = self.queue.receive_messages(MaxNumberOfMessages=10, VisibilityTimeout=120, WaitTimeSeconds=5)
            if len(messages) == 0:
                break

            for message in messages:
                body = json.loads(message.body)
                if 'Records' not in body:
                    print('Bad SQS message body detected going to delete: {}'.format(body))
                    message.delete()
                    continue

                for record in body['Records']:
                    s3_record = record['s3']
                    file = s3_resource.Object(s3_record['bucket']['name'], s3_record['object']['key'])
                    tags = s3_client.get_object_tagging(Bucket=file.bucket_name, Key=file.key)['TagSet']
                    print('File uploaded: {} with tags: {}'.format(file, tags))

                    file_content = file.get()['Body'].read().decode('utf-8')
                    print('Downloaded file contents: {}'.format(file_content))

                    print("Tagging object with the processed date/time.")
                    tags.append({'Key': 'processed', 'Value': self.get_iso_date()})
                    s3_client.put_object_tagging(Bucket=file.bucket_name, Key=file.key, Tagging={'TagSet': tags})

                    print('Acknowledging SQS message')
                    message.delete()

    @staticmethod
    def get_iso_date():
        return datetime.datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%SZ')

    def _upload_random_files_to_s3(self):
        for i in range(random.randint(0, 12)):
            random_uuid = str(uuid.uuid4())
            content = 'Key: {}, generated in iteration cycle {}'.format(random_uuid, i)
            s3_resource.Object('sdavids', random_uuid + '.txt').put(
                Body=content.encode('utf-8'), Tagging=urllib.parse.urlencode({'inserted': self.get_iso_date()})
            )


if __name__ == "__main__":
    lz = LandingZoneProcessor()
    #lz._upload_random_files_to_s3()
    lz.read_s3_messages()
