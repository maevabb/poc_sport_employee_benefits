import great_expectations as gx

context = gx.get_context()

# Retrieve a Data Source
datasource_name = "sport_data_solution"
data_source = context.data_sources.get(datasource_name)

# Get the Data Asset from the Data Source
asset_name = "activities_asset"
data_asset = data_source.get_asset(asset_name)

full_table_batch_definition = data_asset.add_batch_definition_whole_table(
    name="FULL_TABLE"
)

full_table_batch = full_table_batch_definition.get_batch()
print(full_table_batch.head())