# pyCombiner

Helper project to provide lambda-ized T of ETL.  Receive a stream of small objects in S3, clean them up, and spit them out into a single file for Redshift to load.

## Deployment
PRE: Have nodejs and python installed
PRE: Have the AWS CLI set up and configured for the account you want to deploy to
```bash
    pip install awscli
```
PRE: Have Serverless installed (npm install -g serverless)
```bash
    npm install -g serverless
```
Deploy via serverless CLI
```bash
    serverless deploy
```

## Command Line Execution 
If you're running this locally for testing purposes, tweak handler.py with the desired entry point.  It's set to kick off the watcher flow at the moment, and takes a bucket name as a parameter.

## Missing Config File - deploy.env.yml
This project assumes there is a file called deploy.env.yml at the root configured with the following evironment variables:
```yaml
    ENV_VARIABLE: true # Ignore this, it's a placeholder from serverless seed 
    IAM_ROLE: arn:aws:iam::<ACCOUNT_NUMBER>:role/<ROLE_NAME> # IAM Role to deploy with, needs Lambda and S3
    SOURCE_BUCKET: <your_bucket> # Bucket to use for receiving and combining things
``` 

## Event Flow
Somewhere in ETL land, a 10GB file gets streamed out into 10MB chunks
    When the ETL finishes, it writes a manifest to {sourceBucket}/watch/queue
    These chunks write to {sourceBucket}/uploaded/jobId/foo-0001.json through {sourceBucket}/uploaded/jobId/foo-1000.json
    As each chunk lands, it gets picked up, normalized, and deposited into {sourceBucket}/scrubbed
A disconnected Watch job fires off every minute and looks for files in {sourceBucket}/watch/queue (Coming Soon!)
    If there is a job queued AND all files have landed, move the manifest from {sourceBucket}/watch/queue to {sourceBucket}/watch/run and fire the signal to combine them
    This will kick off the combine job, which will stitch all of the files together and dump them in {sourceBucket}/out/jobId/foo.json
    Finally, dump the manifest into {sourceBucket}/watch/done once all lmabda jobs have been submitted

## But wait there's more!
  Early testing showed files had a sweet spot of ~8-12 MB for stitching and ~1GB per 90s
  To avoid even getting close to Lambda timeouts, we will cap file size for first run at 1GB
  If the total combine file size is >1GB (hint, it probably will be), we will spin up a jobId_1 manifest that combines the output of the first job
  First run will create 10 1GB chunks, second run will stitch all of the 1GB chunks together into the final destination

## Pitfalls
    MultiPart upload can't be larger than 10,000 parts -- need to handle second pass of >10 TB (yagni, add this later if it ever comes up)
    MultiPart upload can't attach files larger than 5GB -- only relevant if we need a third pass
    Output files larger than 5GB can't be copied to new and exciting destinations
        investigate copy_part_from_key for moving giganormous files
