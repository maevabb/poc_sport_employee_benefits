import great_expectations as gx

context = gx.get_context()

data_source = context.data_sources.get("s3_rh_datasource")

# Create Table Asset
asset_name = "S3_employes_asset"
s3_prefix = "clean_data/"

s3_file_data_asset = data_source.add_csv_asset(name=asset_name, s3_prefix=s3_prefix)

print(data_source.assets)
