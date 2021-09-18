#!/usr/bin/env python3

import argparse
import json
import os
import subprocess
import sys
import time
import boto3


def transcode(filepath, bucket):
    encode_filepath = f"{os.path.splitext(filepath)[0]}.mp3"
    cmd = ("ffmpeg", "-i", filepath, "-q:a", "0", "-map", "a", encode_filepath)
    print(f"Running '{' '.join(cmd)}'")
    try:
        subprocess.run(
            cmd, stdout=subprocess.DEVNULL, stderr=subprocess.PIPE, check=True
        )
    except subprocess.CalledProcessError as err:
        sys.exit(f"Failed to encode {filepath} to {encode_filepath}: {err.stderr}\n")

    bucket.upload_file(encode_filepath, f"audio/{os.path.basename(encode_filepath)}")


def process_message(msg):
    s3 = boto3.resource("s3")
    event = json.loads(msg["Body"])["Records"][0]
    print(json.dumps(event, indent=2))
    bucket_name = event["s3"]["bucket"]["name"]
    print("Bucket:", bucket_name)
    bucket = s3.Bucket(bucket_name)
    if not event["eventName"].startswith("ObjectCreated"):
        print(f"Skipping event {event['eventName']}", file=sys.stderr)
        sys.exit()
    s3_object = event["s3"]["object"]
    local_filepath = os.path.basename(s3_object["key"])
    bucket.download_file(s3_object["key"], local_filepath)
    transcode(local_filepath, bucket)
    bucket.delete_objects(
        Delete={
            "Objects": [
                {
                    "Key": s3_object["key"],
                }
            ]
        }
    )


def process_messages(messages):
    for msg in messages:
        process_message(msg)


def main():
    sqs = boto3.client("sqs")
    queue_url = sqs.get_queue_url(QueueName=os.getenv("QUEUE_NAME"))["QueueUrl"]
    while True:
        sleeptime = 600
        print("Sleeping", sleeptime, "seconds")
        time.sleep(sleeptime)
        data = sqs.receive_message(QueueUrl=queue_url, WaitTimeSeconds=20)
        print(json.dumps(data))
        try:
            messages = data["Messages"]
        except KeyError:
            print("Not an expected event type. Skipping it", file=sys.stderr)
            continue
        else:
            process_messages(messages)
        finally:
            if data and data.get("ReceiptHandle", None) is not None:
                receipt_handle = data["ReceiptHandle"]
                sqs.delete_message(QueueUrl=queue_url, ReceiptHandle=receipt_handle)
    print("All done. Exiting")


if __name__ == "__main__":
    main()
