# Set up paths to support loading from /vendored
import sys, os
here = os.path.dirname(os.path.realpath(__file__))
sys.path.append(os.path.join(here, "./"))
sys.path.append(os.path.join(here, "./vendored"))

# Rest of imports
import boto3
import json
import logging
import smart_open

lambdaClient = boto3.client('lambda')
s3 = boto3.client('s3')
logging.basicConfig(format='%(asctime)s => %(message)s')

# S3 multi-part upload parts must be larger than 5mb
MIN_S3_SIZE = 5500000


def evaluateWatcher(bucket, key):
    basePath = "s3://" + bucket + "/"
    logging.warning(basePath + key['Key'])
    with smart_open.smart_open(basePath + key['Key']) as fin:
        configuration = json.loads(fin.read().decode("utf-8"))
        logging.warning("Found and read config file: %s", configuration)

    logging.warning("Looking for %s files in %s", configuration['fileCount'], configuration['source'])
    watchedParts = collect_parts( bucket, configuration['source'])
    if configuration['fileCount'] == len(watchedParts):
        logging.warning("Found %s files, trigger combination to %s", configuration['fileCount'], configuration['target'])
        # CP queue to run
        if 'maxFileSize' in configuration:
            maxFileSize = configuration['maxFileSize']
        else:
            maxFileSize = 1073741824 # Default to 1GB
        groupedPartList = chunk_by_size(watchedParts, maxFileSize)
        logging.warning("Assemble %s output files", len(groupedPartList))
        lambdaClient = boto3.client('lambda')
        if (len(groupedPartList) > 1):
            # Write a new manifest
            newConfiguration = {}
            if "iteration" in configuration:
                newConfiguration['iteration'] = configuration['iteration'] + 1
            else:
                newConfiguration['iteration'] = 1
            newConfiguration['fileCount'] = len(groupedPartList)
            newConfiguration['target'] = configuration['target']
            newConfiguration['source'] = configuration['source'] + "_" + str(newConfiguration['iteration'])
            newConfiguration['maxFileSize'] = maxFileSize * 4 # Go up to 4GB, if a third loop is needed the files will be too big
            with smart_open.smart_open(basePath + "/watch/queue/test.json", 'wb') as fout:
                fout.write(json.dumps(newConfiguration))
            for index, part in enumerate(groupedPartList):
                event = {}
                logging.warning("Invoke lambda to assemble %s", (str(index) + ".json").zfill(10))
                event['destination'] = newConfiguration['source'] + "/" + (str(index) + ".json").zfill(10)
                event['parts'] = part
                lambdaClient.invoke(
                    FunctionName='aws-python-simple-http-endpoint-dev-combineRunner',
                    InvocationType='Event',
                    LogType='None',
                    Payload=str.encode(json.dumps(event))
                )

        else:
            logging.warning("Single Output File, Deliver to Final Destination")
            event = {}
            event['destination'] = configuration['target']
            event['parts'] = groupedPartList[0]
            lambdaClient.invoke(
                FunctionName='aws-python-simple-http-endpoint-dev-combineRunner',
                InvocationType='Event',
                LogType='None',
                Payload=str.encode(json.dumps(event))
            )
    else:
        logging.warning("Found %s files, awaiting next cycle", len(watchedParts))
    return


def collect_parts(bucket, prefix):
    """
    collect parts
    """
    return filter(lambda x: x[0].endswith(".json"), list_all_objects_with_size(bucket, prefix))


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
    resp = s3.list_objects(Bucket=bucket, Prefix=prefix)
    if 'Contents' in resp:
        objects_list.extend(resp_to_filelist(resp))
    while resp['IsTruncated']:
        # if there are more entries than can be returned in one request, the key
        # of the last entry returned acts as a pagination value for the next
        # request
        logging.warning("Found %s objects so far", len(objects_list))
        last_key = objects_list[-1][0]
        resp = s3.list_objects(Bucket=bucket, Prefix=prefix, Marker=last_key)
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
        if current_size > max_filesize:
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
        upload_id = initiate_concatenation(bucket, result_filepath)
        parts_mapping = assemble_parts_to_concatenate(
            result_filepath, upload_id, bucket, parts_list)
        complete_concatenation(result_filepath, upload_id, bucket, parts_mapping)
    elif len(parts_list) == 1:
        # can perform a simple S3 copy since there is just a single file
        resp = s3.copy_object(
            Bucket=bucket, CopySource="{}/{}".format(bucket, parts_list[0][0]), Key=result_filepath)
        logging.warning(
            "Copied single file to %s and got response %s", result_filepath, resp)
    else:
        logging.warning("No files to concatenate for %s", result_filepath)

def initiate_concatenation(bucket, result_filename):
    """
    performing the concatenation in S3 requires creating a multi-part upload
    and then referencing the S3 files we wish to concatenate as "parts" of
    that upload
    """
    resp = s3.create_multipart_upload(Bucket=bucket, Key=result_filename)
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
        s3.abort_multipart_upload(
            Bucket=bucket, Key=result_filename, UploadId=upload_id)
        logging.warning(
            "Aborted concatenation for file %s, with upload id #%s due to empty parts mapping",
            result_filename, upload_id)
    else:
        s3.complete_multipart_upload(
            Bucket=bucket,
            Key=result_filename,
            UploadId=upload_id,
            MultipartUpload={'Parts': parts_mapping})
        logging.warning(
            "Finished concatenation for file %s, with upload id #%s, and parts mapping: %s",
            result_filename, upload_id, parts_mapping)

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
        resp = s3.upload_part_copy(Bucket=bucket,
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
        s3.download_file(Bucket=bucket, Key=source_part,
                           Filename=temp_filename)

        with open(temp_filename, 'rb') as f:
            small_parts.append(f.read())
        os.remove(temp_filename)
        logging.warning(
            "Downloaded and copied small part with path: %s", source_part)

    if len(small_parts) > 0:
        last_part_num = part_num + 1
        last_part = ''.join(small_parts)
        resp = s3.upload_part(Bucket=bucket, Key=result_filename,
                                PartNumber=last_part_num, UploadId=upload_id, Body=last_part)
        logging.warning(
            "Setup local part #%s from %s small files, and got response: %s",
            last_part_num, len(small_parts), resp)
        parts_mapping.append(
            {'ETag': resp['ETag'][1:-1], 'PartNumber': last_part_num})

    return parts_mapping

