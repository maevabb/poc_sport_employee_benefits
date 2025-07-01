import great_expectations as gx

context = gx.get_context()

expectation_suite_name = "validate_activities_sportives"
expectation_suite = context.suites.get(name=expectation_suite_name)

# Définir un checkpoint temporaire
checkpoint = context.add_or_update_checkpoint(
    name="checkpoint_activities_sportives",
    validations=[
        {
            "batch_request": {
                "datasource_name": "postgres_conn",
                "data_asset_name": "activities_sportives_asset",
            },
            "expectation_suite_name": "validate_activities_sportives",
        }
    ],
)

# Lancer la validation
results = checkpoint.run()

# Résultat résumé
success = results["success"]
print(f"✅ Validation terminée - Succès : {success}")