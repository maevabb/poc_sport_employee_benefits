import great_expectations as gx

context = gx.get_context()

validation_definition_name = "validate_activities_data"
validation_definition = context.validation_definitions.get(validation_definition_name)

validation_results = validation_definition.run()

print(validation_results)

