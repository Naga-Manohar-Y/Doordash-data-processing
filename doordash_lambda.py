import boto3
import json
import pandas as pd

s3_client = boto3.client('s3')
sns_client = boto3.client('sns')
sns_arn = 'arn:aws:sns:us-east-1:484478793255:doordash-file-arrival'

def lambda_handler(event, context):
    print(event)
    try:
        #TODO implement
        bucket_name = event["Records"][0]["s3"]["bucket"]["name"]
        file_name = event["Records"][0]["s3"]["object"]['key']
        print(bucket_name)
        print(file_name)
        json_object = s3_client.get_object(Bucket=bucket_name, Key=file_name)
        json_data = json.load(json_object['Body'])
        doordash_df = pd.DataFrame(json_data)
        print(doordash_df)
        # Filtering the DataFrame based on 'status' column
        delivered_df = doordash_df[doordash_df['status'] == 'delivered']
    
        #print(delivered_df)
        delivered_json = delivered_df.to_json(orient='records')
        print(delivered_json)
    
        dest_bucket_name = 'doordash-target-zn-vyas'
        dest_file_name = 'doordash_delivered_data.json'
    
        # Upload the filtered JSON data to the destination S3 bucket
        s3_client.put_object(
        Bucket=dest_bucket_name,
        Key=dest_file_name,
        Body=delivered_json,
        ContentType='application/json'
        )
        message = "Doordash records on {} has been processed succesfuly !!".format("s3://"+bucket_name+"/"+file_name)
        respone = sns_client.publish(Subject="SUCCESS - Daily Data Processing",TargetArn=sns_arn, Message=message, MessageStructure='text')
        
    except Exception as err:
        print(err)
        message = "Doordash records on {} processing is Failed !!".format("s3://"+bucket_name+"/"+file_name)
        respone = sns_client.publish(Subject="FAILED - Daily Data Processing", TargetArn=sns_arn, Message=message, MessageStructure='text')