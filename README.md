# Datalake-AWS-lambda-functions
This repository contains lambda functions that can validate the metadata tags attached to s3 object and on the basis of validation it copies the file to respective s3 buckets with proper metadata tags by running cli command on ec2 instance.

Validation is done on the basis of two factors i.e.
a. those objects should contains list of metadata (defined by user)
b. those metadata tags should have a values, they should not be empty.
