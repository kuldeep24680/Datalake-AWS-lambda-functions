/* --- this lambda function validates the list of metadata attached to the object in s3 and if that validation pass the file is copied to destination bucket through cli command and if the validation fails the file will be copied to failure bucket.
validation is based on two factors i.e. availablity of required metadata tags and other is each metadata tag should have its value and should not be empty ----*/



var AWS = require('aws-sdk');
var s3 = new AWS.S3();
var SSH = require('simple-ssh');
var fs = require("fs");



exports.handler = function(event, context,callback){

var bucket = event.Records[0].s3.bucket.name;
var key = decodeURIComponent(event.Records[0].s3.object.key.replace(/\+/g, ' '));
//const region = event.Records[0].awsRegion;
var dest = '<destination bucket>'; 
var fail= '<failure case bucket>';


var usecase,name,quality,email_Id,bucket_owner,url;

var url,prefix='',meta_url;
var length=0;
var s3FileCommand_success,s3FileCommand_fail,s3FileCommand_fail_prefix;
console.log (key);
 //headObject is used to retrieve the properties of object in s3 bucket
    s3.headObject(
        {
            Bucket : bucket,
            Key: key
        },
        function(err, data)
        {
            if (err)
            {
                console.log(err);
                context.done('Error', 'Error getting s3 object: ' + err);
            }
            else
            {          /* -- create SSH object wit the credentials that you need to connect to your EC2 instance -- */
                    var ssh = new SSH({
                        host: '<private ip of ec2 instance>',
                        user: 'ec2-user',
                        key: fs.readFileSync("file.pem") // file.pem is the .pem file attached in deployment package to ssh into ec2 instance 
                });
                var response=JSON.stringify(this.httpResponse.headers);
                console.log("response"+response.replace(/\"/g, ""));
               if(!response.includes('x-amz-meta-retention') || !response.includes('x-amz-meta-usecase') || !response.includes('x-amz-meta-status') || !response.includes('x-amz-meta-business') || !response.includes('x-amz-meta-quality') || !response.includes('x-amz-meta-refresh_velocity') || !response.includes('x-amz-meta-source') || !response.includes('x-amz-meta-ingestion_owner') || !response.includes('x-amz-meta-confidentiality') || !response.includes('x-amz-meta-expiration') || !response.includes('x-amz-meta-level1_ownership') || !response.includes('x-amz-meta-level2_ownership') || !response.includes('x-amz-meta-email_id') || !response.includes('x-amz-meta-url')){
                
               
                 s3FileCommand_fail =   'sudo /usr/local/bin/aws s3 cp s3://' + bucket + '/' + key + ' s3://' + fail + '/'  + ' --sse';
                /* -- execute SSH command -- */
                ssh.exec('cd /myfolder/mysubfolder').exec('ls -al', {
                out: function(stdout) {
                                        console.log('ls -al got:');
                                         console.log(stdout);
                                         console.log('now launching command');
                                         console.log(s3FileCommand_fail);
                                        }
                }).exec('' + s3FileCommand_fail, {
                out: console.log.bind(console),
                exit: function(code, stdout, stderr) {
                console.log('operation exited with code: ' + code);
                console.log('STDOUT from EC2:\n' + stdout);
                console.log('STDERR from EC2:\n' + stderr);
                context.succeed('Success!');
                }
                }).start();
                }
                else{
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
                
                 name = JSON.stringify(this.httpResponse.headers['x-amz-meta-name']);
                 use_case = JSON.stringify(this.httpResponse.headers['x-amz-meta-usecase']);
                 quality = JSON.stringify(this.httpResponse.headers['x-amz-meta-quality']);
                 bucket_owner = JSON.stringify(this.httpResponse.headers['x-amz-meta-bucket_owner']); 
                 email_Id=JSON.stringify(this.httpResponse.headers['x-amz-meta-email_id']);
                 meta_url=bucket+'/'+ key;
                 console.log ("Retention"+retention.replace(/\"/g, ""));
                 console.log ("status"+status.replace(/\"/g, ""));
                 console.log ("business"+business.replace(/\"/g, ""));
                 console.log ("use_case"+use_case.replace(/\"/g, ""));
                 console.log ("meta_quality"+quality.replace(/\"/g, ""));
                 console.log ("refresh_velocity"+refresh_velocity.replace(/\"/g, ""));
                 console.log ("source"+source.replace(/\"/g, ""));
                 console.log ("ingestion_owner"+ingestion_owner.replace(/\"/g, ""));
                 console.log ("meta_confidentiality"+confidentiality.replace(/\"/g, ""));
                 console.log ("expiration"+expiration.replace(/\"/g, ""));
                 console.log ("level_1_ownership"+level1_ownership.replace(/\"/g, ""));
                 console.log ("level_2_ownership"+level2_ownership.replace(/\"/g, ""));
                 console.log ("email_Id"+email_Id.replace(/\"/g, ""));
                 console.log ("url"+meta_url);
                  
                  
                  // cli command to be written into string in order to run on ec2 instance
                  
                  
                  //success case cli command
                  s3FileCommand_success =  'sudo /usr/local/bin/aws s3 cp s3://' + bucket + '/' + key + ' s3://' + dest +'/' + prefix+ ' --metadata '+ "'{"+'"usecase":'+use_case+','+'"name":'+name+','+'"bucket_owner":'+bucket_owner+','+'"email_Id":'+email_Id+','+'"quality":'+quality+','+'"url":'+'"'+meta_url+'"'+"}'"+ ' --sse';
                  
                  //failure case cli command
                  s3FileCommand_fail_prefix =  'sudo /usr/local/bin/aws s3 cp s3://' + bucket + '/' + key + ' s3://' + fail +'/' + prefix+ ' --metadata '+ "'{"+'"usecase":'+use_case+','+'"name":'+name+','+'"bucket_owner":'+bucket_owner+','+'"email_Id":'+email_Id+','+'"quality":'+quality+','+'"url":'+'"'+meta_url+'"'+"}'"+ ' --sse';

                 
                  console.log("before validation:"+Date.now());
                if(retention .replace(/\"/g, "") ===""  || business.replace(/\"/g, "") ==="" || refresh_velocity.replace(/\"/g, "") ===""  || source.replace(/\"/g, "") ==="" || status.replace(/\"/g, "") ===""
                || ingestion_owner.replace(/\"/g, "") ==="" || confidentiality.replace(/\"/g, "") ==="" || expiration.replace(/\"/g, "") ==="" 
                || level1_ownership.replace(/\"/g, "") ==="" || level2_ownership.replace(/\"/g, "") ==="")
                {
                    
                
                /* -- execute SSH command -- */
                ssh.exec('cd /myfolder/mysubfolder').exec('ls -al', {
                out: function(stdout) {
                                        console.log('ls -al got:');
                                         console.log(stdout);
                                         console.log('now launching command');
                                         console.log(s3FileCommand_fail_prefix);
                                        }
                }).exec('' + s3FileCommand_fail_prefix, {
                out: console.log.bind(console),
                exit: function(code, stdout, stderr) {
                console.log('operation exited with code: ' + code);
                console.log('STDOUT from EC2:\n' + stdout);
                console.log('STDERR from EC2:\n' + stderr);
                context.succeed('Success!');
                }
                }).start();
       

                }
                else
                { 
                    
                   
                /* -- execute SSH command -- */
                ssh.exec('cd /myfolder/mysubfolder').exec('ls -al', {
                out: function(stdout) {
                                        console.log('ls -al got:');
                                         console.log(stdout);
                                         console.log('now launching command');
                                         console.log(s3FileCommand_success);
                                        }
                }).exec('' +s3FileCommand_success, {
                out: console.log.bind(console),
                exit: function(code, stdout, stderr) {
                console.log('operation exited with code: ' + code);
                console.log('STDOUT from EC2:\n' + stdout);
                console.log('STDERR from EC2:\n' + stderr);
                context.succeed('Success!');
                }
                }).start();
       
       
                }
                }
            }
        }
    );
      
       callback(null,"Successfully Executed!!");
};


