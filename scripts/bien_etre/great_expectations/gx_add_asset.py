import great_expectations as gx

context = gx.get_context()

data_source = context.data_sources.get("bdd_postgresql")

# Create Table Asset
asset_name = "activities_asset"
database_table_name = "activities_sportives"

table_data_asset = data_source.add_table_asset(
    table_name=database_table_name, name=asset_name
)

print(data_source.assets)
