import great_expectations as gx

def run_checkpoint_employe():
    """
    Exécute le checkpoint Great Expectations 'validate_activities_checkpoint'.
    Lève une erreur si la validation échoue.
    """
    context = gx.get_context()
    checkpoint = context.checkpoints.get("validate_activities_checkpoint")
    validation_results = checkpoint.run()

    if not validation_results["success"]:
        raise ValueError("❌ La validation du checkpoint 'validate_activities_checkpoint' a échoué.")
    
    print("✅ Validation GX activities réussie.")
