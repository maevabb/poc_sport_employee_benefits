import great_expectations as gx

def run_checkpoint_activities():
    """
    Exécute le checkpoint Great Expectations 'validate_activities_checkpoint'.
    Lève une erreur si la validation échoue.
    """
    context = gx.get_context(context_root_dir="/opt/airflow/great_expectations")
    checkpoint = context.checkpoints.get("validate_activities_checkpoint")
    result = checkpoint.run()

    if not result.success:
        raise ValueError("❌ La validation du checkpoint 'validate_activities_checkpoint' a échoué.")
    
    print("✅ Validation GX activities réussie.")
