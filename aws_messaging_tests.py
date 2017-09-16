#!/usr/bin/env python3
import boto3
import unittest
from aws_messaging import LandingZoneProcessor
from moto import mock_s3
from moto import mock_sqs
import json
import urllib


class LandingZoneProcessorTests(unittest.TestCase):

    bucket_name = 'sdavids'
    queue_name = 'sdavids-bucket-create-events'

    def setup_mocks(self):
        self.sqs = boto3.resource('sqs')
        self.s3 = boto3.resource('s3')
        self.s3_client = boto3.client('s3')

        self.queue = self.sqs.create_queue(QueueName=self.queue_name)
        self.bucket = self.s3.Bucket(self.bucket_name)
        self.bucket.create()

    @mock_s3
    @mock_sqs
    def test_receive_messages(self):
        self.setup_mocks()
        object_key = 'foo.txt'
        inserted_date = LandingZoneProcessor.get_iso_date()

        self.bucket.put_object(Key=object_key, Body='test body'.encode('utf-8'), Tagging=urllib.parse.urlencode({'inserted': inserted_date}))
        self.queue.send_message(MessageBody=self._generate_s3_message(object_key))

        LandingZoneProcessor().read_s3_messages()

        tags = self.get_tags(object_key)
        self.assertEqual(tags['inserted'], inserted_date, 'S3 object missing the inserted date')
        # Strip off the seconds from the inserted compared to processed
        self.assertEqual(tags['processed'][:-3], inserted_date[:-3], 'S3 object has the wrong processed date')

        self.queue.reload()
        self.assertEqual(self.queue.attributes['ApproximateNumberOfMessages'], '0', 'SQS message was not acknowledged.')
        self.assertEqual(self.queue.attributes['ApproximateNumberOfMessagesNotVisible'], '0', 'SQS message was not acknowledged.')

    def get_tags(self, object_key):
        tags = self.s3_client.get_object_tagging(Bucket=self.bucket_name, Key=object_key)['TagSet']
        return {item['Key']: item['Value'] for item in tags}

    @staticmethod
    def _generate_s3_message(object_key, bucket_name=bucket_name):
        s3_sqs_message = {"Records": [{"eventVersion": "2.0",
                             "eventSource": "aws:s3",
                             "awsRegion": "us-east-1",
                             "eventTime": "2017-09-16T00:51:09.283Z",
                             "eventName": "ObjectCreated:Put",
                             "userIdentity": {"principalId": "AWS:fooidentity"},
                             "requestParameters": {"sourceIPAddress": "192.168.1.1"},
                             "responseElements": {"x-amz-request-id": "07D1949913D03FAE",
                                                  "x-amz-id-2": "tenvcUYAc60FOY4pQJHjZ+pCU/9q7Ke3ZtVVXMvKRmVG0zMom1+rZa8WJryiCo9YCuldSiPMUaA="},
                             "s3": {"s3SchemaVersion": "1.0",
                                    "configurationId": "Yzk3ZTZiMTEtMmM3NS00MDUxLTkwYzQtYWExYWVjZjUzMWY5",
                                    "bucket": {"name": bucket_name,
                                               "ownerIdentity": {"principalId": "fooidentity"},
                                               "arn": "arn:aws:s3:::" + bucket_name},
                                    "object": {"key": object_key,
                                               "size": 73,
                                               "eTag": "97bd6a82ccd50588da26177b6a4199bc",
                                               "sequencer": "0059BC757D33F05FF0"}
                                    }
                             }]}

        return json.dumps(s3_sqs_message, indent=2)


if __name__ == '__main__':
    unittest.main()