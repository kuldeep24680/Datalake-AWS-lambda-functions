import urllib
import boto3,logging
import ast
import json
print('Loading function')
s3 = boto3.client('s3')
S3 = boto3.resource('s3')

def lambda_handler(event, context):
    for record in event['Records']:
    	logger = logging.getLogger()
    	logger.setLevel(logging.INFO)
    
    	target_bucket = 'car-test-s3-datalakebucket'
    	failure_bucket = 'car-test-s3-quarantinebucket'
    	source_bucket = record['s3']['bucket']['name']
    	key = record['s3']['object']['key']
    	print(key)
    	print("all out")
    
    	response = s3.head_object(Bucket=source_bucket, Key=key)
    	logger.info('Response: {}'.format(response))
    	copy_source = {'Bucket':source_bucket, 'Key':key}
    	if not response.get('ResponseMetadata',{}).get('HTTPHeaders',{}).get('x-amz-meta-retention',{}) or not response.get('ResponseMetadata',{}).get('HTTPHeaders',{}).get('x-amz-meta-usecase',{}) or not response.get('ResponseMetadata',{}).get('HTTPHeaders',{}).get('x-amz-meta-quality',{}) or not response.get('ResponseMetadata',{}).get('HTTPHeaders',{}).get('x-amz-meta-refresh_velocity',{}) or not response.get('ResponseMetadata',{}).get('HTTPHeaders',{}).get('x-amz-meta-source',{}) or not response.get('ResponseMetadata',{}).get('HTTPHeaders',{}).get('x-amz-meta-ingestion_owner',{}) or not response.get('ResponseMetadata',{}).get('HTTPHeaders',{}).get('x-amz-meta-confidentiality',{}) or not response.get('ResponseMetadata',{}).get('HTTPHeaders',{}).get('x-amz-meta-expiration',{}) or not response.get('ResponseMetadata',{}).get('HTTPHeaders',{}).get('x-amz-meta-level1_ownership',{}) or not response.get('ResponseMetadata',{}).get('HTTPHeaders',{}).get('x-amz-meta-level2_ownership',{}) or not response.get('ResponseMetadata',{}).get('HTTPHeaders',{}).get('x-amz-meta-email_id',{})  or not response.get('ResponseMetadata',{}).get('HTTPHeaders',{}).get('x-amz-meta-business',{})  or not response.get('ResponseMetadata',{}).get('HTTPHeaders',{}).get('x-amz-meta-status',{}):                           
    	    extra_args_fail = {
        'ACL': 'bucket-owner-full-control',
        'ServerSideEncryption' : 'AES256'
        }
    	    s3.copy(Bucket=failure_bucket, Key=key, CopySource=copy_source, ExtraArgs=extra_args_fail)
        	    
    	    print("successfully uploaded to Quarantine bucket due to missing metadata tags")
    	    
    	else:
    	    retention=response['ResponseMetadata']['HTTPHeaders']['x-amz-meta-retention']
    	    use_case=response['ResponseMetadata']['HTTPHeaders']['x-amz-meta-usecase']
    	    quality=response['ResponseMetadata']['HTTPHeaders']['x-amz-meta-quality']
    	    refresh_velocity=response['ResponseMetadata']['HTTPHeaders']['x-amz-meta-refresh_velocity']
    	    source=response['ResponseMetadata']['HTTPHeaders']['x-amz-meta-source']
    	    ingestion_owner=response['ResponseMetadata']['HTTPHeaders']['x-amz-meta-ingestion_owner']
    	    confidentiality=response['ResponseMetadata']['HTTPHeaders']['x-amz-meta-confidentiality']
    	    expiration=response['ResponseMetadata']['HTTPHeaders']['x-amz-meta-expiration']
    	    level1_ownership=response['ResponseMetadata']['HTTPHeaders']['x-amz-meta-level1_ownership']
    	    level2_ownership=response['ResponseMetadata']['HTTPHeaders']['x-amz-meta-level2_ownership']
    	    email_id=response['ResponseMetadata']['HTTPHeaders']['x-amz-meta-email_id']
    	    business=response['ResponseMetadata']['HTTPHeaders']['x-amz-meta-business']
    	    pid=response['ResponseMetadata']['HTTPHeaders']['x-amz-meta-status']
    	    
    	    metadata = {
        'retention': retention,
        'use-case': use_case,
        'quality': quality,
        'refresh-velocity': refresh_velocity,
        'source': source,
        'ingestion-owner': ingestion_owner,
        'confidentiality': confidentiality,
        'expiration': expiration,
        'level1_ownership': level1_ownership,
        'level2_ownership': level2_ownership,
        'status': pid,
        'business': business,
        'expiration': expiration,
        'email_id': email_id
        }
    	    
    	    extra_args = {
        'ACL': 'bucket-owner-full-control',
        'Metadata' : metadata,
        'ServerSideEncryption' : 'AES256'
        }
    	
    	    metadata = {
        'retention': retention,
        'use-case': use_case,
        'quality': quality,
        'refresh-velocity': refresh_velocity,
        'source': source,
        'ingestion-owner': ingestion_owner,
        'confidentiality': confidentiality,
        'expiration': expiration,
        'level1_ownership': level1_ownership,
        'level2_ownership': level2_ownership,
        'status': pid,
        'business': business,
        'expiration': expiration,
        'email_id': email_id
        }
    
    	    print("retention : " + retention)
    	    print("use_case : " + use_case)
    	    print("quality : " + quality)
    	    print("refresh_velocity : " + refresh_velocity)
    	    print("source : " + source)
    	    print("ingestion_owner : " + ingestion_owner)
    	    print("confidentiality : " + confidentiality)
    	    print("expiration : " + expiration)
    	    print("level1_ownership : " + level1_ownership)
    	    print("level2_ownership : " + level2_ownership)
    	    if retention=='' or use_case=='' or quality=='' or refresh_velocity=='' or source=='' or ingestion_owner=='' or confidentiality=='' or expiration=='' or level1_ownership=='' or level2_ownership=='':
        	    print ("Copying %s from bucket %s to bucket %s ..." % (key, source_bucket, failure_bucket))
        	    s3.copy(Bucket=failure_bucket, Key=key, CopySource=copy_source, ExtraArgs=extra_args)
        	    print("successfully uploaded to Quarantine bucket due to missing metadata values")
    	    else:
        	    print ("Copying %s from bucket %s to bucket %s ..." % (key, source_bucket, target_bucket))
        	    s3.copy(Bucket=target_bucket, Key=key, CopySource=copy_source, ExtraArgs=extra_args)
        	    print("successfully uploaded to Datalake bucket")
        	    
        	
        
