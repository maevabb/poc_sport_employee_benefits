import great_expectations as gx

context = gx.get_context()

data_source_name = "bdd_postgresql"

my_connection_string = "${POSTGRESQL_CONNECTION_STRING}"

data_source = context.data_sources.add_postgres(
    name=data_source_name, connection_string=my_connection_string
)

print(context.data_sources.get(data_source_name))

