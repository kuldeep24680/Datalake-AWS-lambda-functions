# AWS-lambda-functions-to-validate-and-copy-file-to-s3-bucket
This repository contains lambda functions that can validate the metadata tags attached to s3 object and on the basis of validation it copies the file to respective s3 buckets with proper metadata tags by running cli command on ec2 instance.

Also Lambda function in copying_of_files_between_buckets_validate_metadata folder using python boto3 library and this code does not reply on any unmanaged service like EC2 but instead it runs solely on lambda function and can copy a file sized 45-50 GB within the period of 5mins timeout from one bucket to another and it also validates the metadata of the object being copied.




Validation is done on the basis of two factors i.e.
a. those objects should contains list of metadata (defined by user)
b. those metadata tags should have a values, they should not be empty.


