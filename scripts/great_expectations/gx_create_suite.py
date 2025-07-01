import great_expectations as gx

context = gx.get_context()

suite_name = "validate_activities_sportives"
suite = gx.ExpectationSuite(name=suite_name)
suite = context.suites.add(suite)


notnull_expectation = gx.expectations.ExpectColumnValuesToNotBeNull(
    column="id_employe"
)

suite.add_expectation(notnull_expectation)


