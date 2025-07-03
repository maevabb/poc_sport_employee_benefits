import great_expectations as gx

context = gx.get_context()

data_source_name = "s3_rh_datasource"
data_asset_name = "S3_employes_asset"
file_data_asset = context.data_sources.get(data_source_name).get_asset(data_asset_name)

batch_definition_name = "cleaned_employes_latest.csv"
batch_definition_path = "cleaned_employes_latest.csv"

batch_definition = file_data_asset.add_batch_definition_path(
    name=batch_definition_name, path=batch_definition_path
)

batch = batch_definition.get_batch()
print(batch.head())
