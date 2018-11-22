# AWS-lambda-functions-to-validate-and-copy-file-to-s3-bucket
This repository contains lambda functions that can validate the metadata tags attached to s3 object and on the basis of validation it copies the file to respective s3 buckets with proper metadata tags by running cli command on ec2 instance.

Also Lambda function in copying_of_files_between_buckets_validate_metadata folder using python boto3 library and this code does not reply on any unmanaged service like EC2 but instead it runs solely on lambda function and can copy a file sized 45-50 GB within the period of 5mins timeout from one bucket to another and it also validates the metadata of the object being copied.




Validation is done on the basis of two factors i.e.
a. those objects should contains list of metadata (defined by user)
b. those metadata tags should have a values, they should not be empty.

Lambda function in Updating_dynamodb_table_with_s3_object_metadata folder that can read the metadata tags of newly uploaded file to S3 bucket and update the same values in dynamodb table with proper schema.

Lambda function in Indexing_data_in_ES_from_dynamodb_table folder that can read the newly updated/put/deleted data in dynamodb table and index it in AWS elasticsearch through its endpoint.

