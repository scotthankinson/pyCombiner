"""
Handler for a series of pyCombiner Lambda Events
"""
# Set up paths to support loading from /vendored
import json
import logging
import sys
import os
HERE = os.path.dirname(os.path.realpath(__file__))
sys.path.append(os.path.join(HERE, "./"))
sys.path.append(os.path.join(HERE, "./vendored"))

# Rest of imports
import boto3
from lib import combine

__S3 = boto3.client("s3")
logging.basicConfig(format="%(asctime)s => %(message)s")

if "SOURCE_BUCKET" in os.environ:
    BUCKET = os.environ["SOURCE_BUCKET"]
else:
    BUCKET = sys.argv[1]

def scrubber(event, context):
    """
    Perform any object cleanup.
    We currently have none defined, so just move to scrubbed
    """
    logging.warning(
        "Received event scrubber: %s", event)
    try:
        bucket = event["Records"][0]["s3"]["bucket"]["name"]
        source = event["Records"][0]["s3"]["object"]["key"]
        destination = event["Records"][0]["s3"]["object"]["key"].replace("uploaded", "scrubbed")
        # Wait for the object to finish appearing
        # S3 sometimes has not finished its internal replication for small files
        waiter = __S3.get_waiter("object_exists")
        waiter.wait(Bucket=bucket, Key=source)
        # Download the object, stream through it performing cleanup, write it to destination

        # Normalize the filenames from "sub-1" to "sub-n" and use "0001.json" - "nnnn".json
        destination = destination.replace("sub-", "")
        destination_parts = destination.split("/")
        destination_parts[len(destination_parts) - 1] = destination_parts[len(destination_parts) - 1].zfill(10)
        new_destination = ""
        for part in destination_parts:
            new_destination = new_destination + part + "/"
        destination = new_destination[:-1]
        logging.warning("Converted %s to %s", source, destination)


        resp = __S3.copy_object(Bucket=bucket,
                                CopySource="{}/{}".format(bucket, source),
                                Key=destination)
        logging.warning(
            "Scrubbed single file to %s/%s and got response %s", bucket, destination, resp)
        resp = __S3.delete_object(Bucket=bucket, Key=source)
        logging.warning(
            "Removed source file %s/%s and got response %s", bucket, source, resp)
        return resp
    except Exception as e:
        logging.error(e)
        logging.error(
            "Error scrubbing %s from bucket %s.", source, bucket)


def watcher(event, context):
    """
    Watch for progress
    """
    logging.warning(
        "Received event watcher: %s", event)
    results = []
    for key in __S3.list_objects(Bucket=BUCKET, Prefix="watch/queue/")["Contents"]:
        if key["Key"] != "watch/queue/":
            results.append(key["Key"])
            logging.warning("Triggering Watch Event: %s", key["Key"])
            combine.evaluate_watcher(BUCKET, key)

    body = {
        "message": results
    }

    response = {
        "statusCode": 200,
        "body": json.dumps(body)
    }

    return response

def runner(event, context):
    """
    Process Multi-Part Upload
    """
    logging.warning(
        "Received event runner: %s", event)
    combine.run_single_concatenation(event["parts"], BUCKET, event["destination"])


if __name__ == "__main__":
    logging.warning("Launched with %s", sys.argv)
    watcher(None, None)

