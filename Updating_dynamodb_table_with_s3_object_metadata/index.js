var AWS = require('aws-sdk');
var s3 = new AWS.S3();
var dynamodb = new AWS.DynamoDB({
	apiVersion : '2012-08-10'
});

exports.handler = function(event, context, callback) {

	var srcbucket = '<source_bucket_name>';
	var srcKey = event.Records[0].s3.object.key;
	var retention, metadata1,metadata2,prefix,url, emailID,metadata3, metadata4,metadata5, Last_modified, Meta_URL,metadata6, expiration, ETag,metadata10,metadata7 ,metadata8 ,ingestion_owner,metadata9;
	var tableName = "<table_name>";
	var length=0;
	
	s3.headObject(
		            {
						Bucket : srcbucket,
						Key : srcKey
					},
					function(err, data) {
						
						if (err) {
							console.log(err);
							context.done('Error', 'Error getting s3 object: '
									+ err);
						} else {
							
							url = JSON.stringify(this.httpResponse.headers['x-amz-meta-url']);
                if(url.replace(/\"/g, "") ==="" ){
                    prefix="";
                }
                else{
                    
                url=url.split("/");
                length=url.length;
                for (var i=1;i<length-1;i++){
                 prefix=prefix+url[i]+'/';
                 }}
							
							metadata1 = JSON
									.stringify(this.httpResponse.headers['x-amz-meta-metadata1']);
								
						
							Last_modified = JSON
									.stringify(this.httpResponse.headers['last-modified']);
					
						 Meta_URL='https://s3.eu-central-1.amazonaws.com/ttgsl-preprod-datalake-bkt/'+prefix+'/'+srcKey;
							metadata2 = JSON
									.stringify(this.httpResponse.headers['x-amz-meta-metadata2']);
							emailID=JSON
									.stringify(this.httpResponse.headers['x-amz-meta-email_id']);
						metadata3 = JSON.stringify(this.httpResponse.headers['x-amz-meta-metadata3']);
						
							metadata4 =JSON
									.stringify(this.httpResponse.headers['x-amz-meta-metadata4']);
							metadata5 = JSON
									.stringify(this.httpResponse.headers['x-amz-meta-metadata5']);
						    ingestion_owner = JSON
									.stringify(this.httpResponse.headers['x-amz-meta-ingestion_owner']); 
							metadata6 = JSON
									.stringify(this.httpResponse.headers['x-amz-meta-metadata6']); 
							expiration = JSON
									.stringify(this.httpResponse.headers['x-amz-meta-expiration']);
							metadata7 = JSON
									.stringify(this.httpResponse.headers['x-amz-meta-metadata7']);
							metadata8 = JSON
									.stringify(this.httpResponse.headers['x-amz-meta-metadata8']); 
							ETag = JSON
									.stringify(this.httpResponse.headers['etag']);
							metadata9 = JSON
									.stringify(this.httpResponse.headers['x-amz-meta-metadata9']);
							var metadata10 = JSON
									.stringify(this.httpResponse.headers['x-amz-meta-metadata10']);
					
					
						
						

							var params = {
								TableName : tableName,
								Item : {
									'id' : {
										S : srcKey
									},
									'createdDate' : {
										S : Last_modified
									},
									'url' : {
										S : Meta_URL
									},
									'metadata1': {
										S : metadata1
									},
								
									'metadata2' : {
										S : metadata2
									},
										'metadata3' : {
										S : metadata3
									},
										'metadata4' : {
										S : metadata4
									},
									'metadata5' : {
										S : metadata5
									},
									'metadata6' : {
										S : metadata6
									},
									'metadata7' : {
										S : metadata7
									},
									'dataExpiration' : {
										S : expiration
									},'metadata8' : {
										S : metadata8
									},'user' : {
										S : emailID
									},
									'metadata9' : {
										S : metadata9
									}
								}
							};

							dynamodb.putItem(params, function(err, data) {
								if (err) {
									console.log("Error", err);
								} else {
									console.log("Success", data);
								}
							});

						}
					});
	 
    
	callback(null, 'success');
}
