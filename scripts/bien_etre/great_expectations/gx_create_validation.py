import great_expectations as gx

context = gx.get_context()

expectation_suite_name = "validate_activities_sportives"
expectation_suite = context.suites.get(name=expectation_suite_name)

data_source_name = "bdd_postgresql"
data_asset_name = "activities_asset"
batch_definition_name = "FULL_TABLE"
batch_definition = (
    context.data_sources.get(data_source_name)
    .get_asset(data_asset_name)
    .get_batch_definition(batch_definition_name)
)

definition_name = "validate_activities_data"
validation_definition = gx.ValidationDefinition(
    data=batch_definition, suite=expectation_suite, name=definition_name
)

validation_definition = context.validation_definitions.add(validation_definition)