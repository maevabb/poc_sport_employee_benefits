import boto3

s3 = boto3.client("s3")
response = s3.list_objects_v2(Bucket="p12-sport-data-solution", Prefix="clean_data/")
for obj in response.get("Contents", []):
    print(obj["Key"])
