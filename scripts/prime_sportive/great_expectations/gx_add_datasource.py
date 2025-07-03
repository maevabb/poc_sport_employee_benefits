import great_expectations as gx

context = gx.get_context()

data_source_name = "s3_rh_datasource"
bucket_name = "p12-sport-data-solution"
boto3_options = {}


data_source = context.data_sources.add_pandas_s3(
    name=data_source_name, bucket=bucket_name, boto3_options=boto3_options
)

print(context.data_sources.get(data_source_name))

