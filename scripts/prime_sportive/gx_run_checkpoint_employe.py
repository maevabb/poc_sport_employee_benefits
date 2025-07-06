import great_expectations as gx

def run_checkpoint_employe():
    """
    Exécute le checkpoint Great Expectations 'validate_employes_checkpoint'.
    Lève une erreur si la validation échoue.
    """
    context = gx.get_context(context_root_dir="/opt/airflow/great_expectations")
    checkpoint = context.checkpoints.get("validate_employes_checkpoint")
    validation_results = checkpoint.run()

    if not validation_results["success"]:
        raise ValueError("❌ La validation du checkpoint 'validate_employes_checkpoint' a échoué.")
    
    print("✅ Validation GE employé réussie.")
