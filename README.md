# pyCombiner

## Receive and clean a stream of small objects in S3
## Roll those objects up into a single result file
## ?? 
## Profit

# Event Flow
## Somewhere in ETL land, a 10GB file gets streamed out into 10MB chunks
### When the ETL finishes, it writes a manifest to {sourceBucket}/watch/queue
### These chunks write to {sourceBucket}/uploaded/jobId/foo-0001.json through {sourceBucket}/uploaded/jobId/foo-1000.json
### As each chunk lands, it gets picked up, normalized, and deposited into {sourceBucket}/scrubbed
## A disconnected Watch job fires off every minute and looks for files in {sourceBucket}/watch/queue
### If there is a job queued AND all files have landed, move {sourceBucket}/watch/queue to {sourceBucket}/watch/run
### This will kick off the combine job, which will stitch all of the files together and dump them in {sourceBucket}/out/jobId/foo.json

## But wait there's more!
### Early testing showed files had a sweet spot of ~8-12 MB for stitching and ~1GB per 80s
### To avoid even getting close to Lambda timeouts, we will cap file sizes at 1GB, 2GB, and 4GB
### Suppose foo.json was ~3.5GB, it will shard results out to {sourceBucket}/uploaded/jobId_1/foo-0001.json - {sourceBucket}/uploaded/jobId_1/foo-0004.json
### And write a new manifest with double the file size.

# Pitfalls
## MultiPart upload can't be larger than 10,000 parts
## MultiPart upload can't attach files larger than 5GB
## Output files larger than 5GB can't be copied to new an exciting destinations
## investigate copy_part_from_key for moving giganormous files
