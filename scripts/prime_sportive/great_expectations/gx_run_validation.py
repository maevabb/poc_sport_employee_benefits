import great_expectations as gx

context = gx.get_context()

validation_definition_name = "validate_employes_data"
validation_definition = context.validation_definitions.get(validation_definition_name)

validation_results = validation_definition.run()

print(validation_results)

