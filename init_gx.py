import great_expectations as gx
import os

# Création d'un projet GX dans un sous-dossier
context = gx.get_context(mode="file", context_root_dir="great_expectations")
print("Contexte initialisé :", type(context).__name__)