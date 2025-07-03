import great_expectations as gx

context = gx.get_context()

expectation_suite_name = "validate_employes"
expectation_suite = context.suites.get(name=expectation_suite_name)

data_source_name = "s3_rh_datasource"
data_asset_name = "S3_employes_asset"
batch_definition_name = "cleaned_employes_latest.csv"
batch_definition = (
    context.data_sources.get(data_source_name)
    .get_asset(data_asset_name)
    .get_batch_definition(batch_definition_name)
)

definition_name = "validate_employes_data"
validation_definition = gx.ValidationDefinition(
    data=batch_definition, suite=expectation_suite, name=definition_name
)

validation_definition = context.validation_definitions.add(validation_definition)