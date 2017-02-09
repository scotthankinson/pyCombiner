"""
Logic to combine multiple small files into a single larger file
"""
# Set up paths to support loading from /vendored
import json
import logging
import os
import sys
HERE = os.path.dirname(os.path.realpath(__file__))
sys.path.append(os.path.join(HERE, "./"))
sys.path.append(os.path.join(HERE, "./vendored"))

# Rest of imports located under ./vendored
import boto3
import smart_open

LAMBDA_CLIENT = boto3.client('lambda')
__S3 = boto3.client('s3')
logging.basicConfig(format='%(asctime)s => %(message)s')

# S3 multi-part upload parts must be larger than 5mb
MIN_S3_SIZE = 5500000


def evaluate_watcher(bucket, key):
    """
    Check to see if all file parts have arrived
    """
    base_path = "s3://" + bucket + "/"
    logging.warning(base_path + key['Key'])
    with smart_open.smart_open(base_path + key['Key']) as fin:
        configuration = json.loads(fin.read().decode("utf-8"))
        logging.warning("Found and read config file: %s", configuration)

    logging.warning("Looking for %s files in %s",
                    configuration['fileCount'],
                    configuration['source'])
    watched_parts = collect_parts(bucket, configuration['source'] + "/")
    if configuration['fileCount'] == len(watched_parts):
        process_combination(configuration, bucket, key['Key'], watched_parts)
    else:
        # TODO track the number of misses, alert if >10?
        logging.warning("Found %s files, awaiting next cycle", len(watched_parts))
    return


def process_combination(configuration, bucket, key, watched_parts):
    """
    Stitch together file parts
    """
    logging.warning("Found %s files, trigger combination to %s",
                    configuration['fileCount'], configuration['target'])
    move_file_in_bucket(bucket, key, "/queue", "/run")

    # determine file size
    if 'maxFileSize' in configuration:
        max_file_size = configuration['maxFileSize']
    else:
        max_file_size = 1073741824 # Default to 1GB
    grouped_part_list = chunk_by_size(watched_parts, max_file_size)
    logging.warning("Assemble %s output files", len(grouped_part_list))
    if len(grouped_part_list) > 1:
        # Write a new manifest
        new_conf = {}
        if "iteration" in configuration:
            new_conf['iteration'] = configuration['iteration'] + 1
        else:
            new_conf['iteration'] = 1
        new_conf['fileCount'] = len(grouped_part_list)
        new_conf['target'] = configuration['target']
        new_conf['source'] = configuration['source'] + "_" + str(new_conf['iteration'])
        # Go up to 4GB, if a third loop is needed the files will be too big
        new_conf['maxFileSize'] = max_file_size * 4
        new_file = key.replace('.json', '_' + str(new_conf['iteration']) + '.json')
        base_path = "s3://" + bucket + "/"
        with smart_open.smart_open(base_path + new_file, 'wb') as fout:
            fout.write(json.dumps(new_conf))
        for index, part in enumerate(grouped_part_list):
            event = {}
            logging.warning("Invoke lambda to assemble %s", (str(index) + ".json").zfill(10))
            event['destination'] = new_conf['source'] + "/" + (str(index) + ".json").zfill(10)
            event['parts'] = part
            LAMBDA_CLIENT.invoke(
                FunctionName='aws-python-simple-http-endpoint-dev-combineRunner',
                InvocationType='Event',
                LogType='None',
                Payload=str.encode(json.dumps(event))
            )
    else:
        logging.warning("Single Output File, Deliver to Final Destination")
        event = {}
        event['destination'] = configuration['target']
        event['parts'] = grouped_part_list[0]
        LAMBDA_CLIENT.invoke(
            FunctionName='aws-python-simple-http-endpoint-dev-combineRunner',
            InvocationType='Event',
            LogType='None',
            Payload=str.encode(json.dumps(event))
        )
    move_file_in_bucket(bucket, key.replace("/queue", "/run"), "/run", "/done")
    return


def collect_parts(bucket, prefix):
    """
    collect parts
    """
    return [x for x in list_all_objects_with_size(bucket, prefix) if x[0].endswith(".json")]
    # return filter(lambda x: x[0].endswith(".json"), list_all_objects_with_size(bucket, prefix))


def list_all_objects_with_size(bucket, prefix):
    """
    list all objects with size
    """

    def resp_to_filelist(resp):
        """
        response to fileList
        """
        return [(x['Key'], x['Size']) for x in resp['Contents']]

    objects_list = []
    resp = __S3.list_objects(Bucket=bucket, Prefix=prefix)
    if 'Contents' in resp:
        objects_list.extend(resp_to_filelist(resp))
    while resp['IsTruncated']:
        # if there are more entries than can be returned in one request, the key
        # of the last entry returned acts as a pagination value for the next
        # request
        logging.warning("Found %s objects so far", len(objects_list))
        last_key = objects_list[-1][0]
        resp = __S3.list_objects(Bucket=bucket, Prefix=prefix, Marker=last_key)
        objects_list.extend(resp_to_filelist(resp))

    return objects_list

def chunk_by_size(parts_list, max_filesize):
    """
    chunk by size
    """
    grouped_list = []
    current_list = []
    current_size = 0
    for __p in parts_list:
        current_size += __p[1]
        current_list.append(__p)
        # Max multi-part upload size is 10,000
        if current_size > max_filesize or len(current_list) == 9999:
            grouped_list.append(current_list)
            current_list = []
            current_size = 0
    # Catch any remainder
    if current_list.count > 0:
        grouped_list.append(current_list)

    return grouped_list

def run_single_concatenation(parts_list, bucket, result_filepath):
    """
    run single concatenation
    """
    if len(parts_list) > 1:
        # perform multi-part upload
        # TODO: Serial calls to lambda with current index
        upload_id = initiate_concatenation(bucket, result_filepath)
        parts_mapping = assemble_parts_to_concatenate(
            result_filepath, upload_id, bucket, parts_list)
        complete_concatenation(result_filepath, upload_id, bucket, parts_mapping)
    elif len(parts_list) == 1:
        # can perform a simple S3 copy since there is just a single file
        resp = __S3.copy_object(
            Bucket=bucket, CopySource="{}/{}".format(bucket, parts_list[0][0]), Key=result_filepath)
        logging.warning(
            "Copied single file to %s and got response %s", result_filepath, resp)
    else:
        logging.warning("No files to concatenate for %s", result_filepath)
    return

def initiate_concatenation(bucket, result_filename):
    """
    performing the concatenation in S3 requires creating a multi-part upload
    and then referencing the S3 files we wish to concatenate as "parts" of
    that upload
    """
    resp = __S3.create_multipart_upload(Bucket=bucket, Key=result_filename)
    logging.warning(
        "Initiated concatenation attempt for %s, and got response: %s",
        result_filename,
        resp)
    return resp['UploadId']

def complete_concatenation(result_filename, upload_id, bucket, parts_mapping):
    """
    complete concatenation
    """
    if len(parts_mapping) == 0:
        __S3.abort_multipart_upload(
            Bucket=bucket, Key=result_filename, UploadId=upload_id)
        logging.warning(
            "Aborted concatenation for file %s, with upload id #%s due to empty parts mapping",
            result_filename, upload_id)
    else:
        __S3.complete_multipart_upload(
            Bucket=bucket,
            Key=result_filename,
            UploadId=upload_id,
            MultipartUpload={'Parts': parts_mapping})
        logging.warning(
            "Finished concatenation for file %s, with upload id #%s, and parts mapping: %s",
            result_filename, upload_id, parts_mapping)
    return

def assemble_parts_to_concatenate(result_filename, upload_id, bucket, parts_list):
    """
    assemble parts to concatenate
    """
    parts_mapping = []
    part_num = 0

    s3_parts = ["{}/{}".format(bucket, p[0])
                for p in parts_list if p[1] > MIN_S3_SIZE]
    local_parts = [p[0] for p in parts_list if p[1] <= MIN_S3_SIZE]

    # assemble parts large enough for direct S3 copy
    # part numbers are 1 indexed
    for part_num, source_part in enumerate(s3_parts, 1):
        resp = __S3.upload_part_copy(Bucket=bucket,
                                     Key=result_filename,
                                     PartNumber=part_num,
                                     UploadId=upload_id,
                                     CopySource=source_part)
        logging.warning(
            "Setup S3 part #%s, with path: %s, and got response: %s",
            part_num, source_part, resp)
        parts_mapping.append(
            {'ETag': resp['CopyPartResult']['ETag'][1:-1], 'PartNumber': part_num})

    # assemble parts too small for direct S3 copy by downloading them locally,
    # combining them, and then reuploading them as the last part of the
    # multi-part upload (which is not constrained to the 5mb limit)
    small_parts = []
    for source_part in local_parts:
        temp_filename = "/tmp/{}".format(source_part.replace("/", "_"))
        __S3.download_file(Bucket=bucket,
                           Key=source_part,
                           Filename=temp_filename)

        with open(temp_filename, 'rb') as __f:
            small_parts.append(__f.read())
        os.remove(temp_filename)
        logging.warning(
            "Downloaded and copied small part with path: %s", source_part)

    if len(small_parts) > 0:
        last_part_num = part_num + 1
        last_part = ''.join(small_parts)
        resp = __S3.upload_part(Bucket=bucket,
                                Key=result_filename,
                                PartNumber=last_part_num,
                                UploadId=upload_id,
                                Body=last_part)
        logging.warning(
            "Setup local part #%s from %s small files, and got response: %s",
            last_part_num, len(small_parts), resp)
        parts_mapping.append(
            {'ETag': resp['ETag'][1:-1], 'PartNumber': last_part_num})

    return parts_mapping

def move_file_in_bucket(bucket, key, source, dest):
    """
    Helper function to move queued events through buckets
    """
    resp = __S3.copy_object(Bucket=bucket,
                            CopySource="{}/{}".format(bucket, key),
                            Key=key.replace(source, dest))
    logging.warning(
        "Initiated transfer for %s and got response %s", "{}/{}".format(bucket, key),
        resp)
    resp = __S3.delete_object(Bucket=bucket, Key=key)
    logging.warning(
        "Deleted source event %s and got response %s", "{}/{}".format(bucket, key), resp)
    return
