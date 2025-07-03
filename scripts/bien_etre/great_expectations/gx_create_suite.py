import great_expectations as gx

context = gx.get_context()

suite_name = "validate_activities_sportives"
suite = gx.ExpectationSuite(name=suite_name)

suite = context.suites.add(suite)

# Colonnes non-nulles
suite.add_expectation(gx.expectations.ExpectColumnValuesToNotBeNull(column="id_employe"))
suite.add_expectation(gx.expectations.ExpectColumnValuesToNotBeNull(column="debut_activite"))
suite.add_expectation(gx.expectations.ExpectColumnValuesToNotBeNull(column="fin_activite"))
suite.add_expectation(gx.expectations.ExpectColumnValuesToNotBeNull(column="type_activite"))
suite.add_expectation(gx.expectations.ExpectColumnValuesToNotBeNull(column="temps_sec"))

# Distance et durée positives
suite.add_expectation(gx.expectations.ExpectColumnMaxToBeBetween(
    column="distance",
    min_value= 0,
))
suite.add_expectation(gx.expectations.ExpectColumnMaxToBeBetween(
    column="temps_sec",
    min_value= 0,
))

# Date de fin >= Date début
suite.add_expectation(gx.expectations.ExpectColumnPairValuesAToBeGreaterThanB(
    column_A="debut_activite",
    column_B="fin_activite",
    or_equal=True
))





