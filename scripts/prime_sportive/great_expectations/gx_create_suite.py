import great_expectations as gx
from datetime import date

context = gx.get_context()

suite_name = "validate_employes"
suite = gx.ExpectationSuite(name=suite_name)

suite = context.suites.add(suite)

# Présence des colonnes
columns = [
    "id_employe", "nom", "prenom", "date_naissance", "bu",
    "date_embauche", "salaire_brut", "type_contrat", "nb_jours_cp",
    "adresse_domicile", "mode_deplacement", "pratique_sportive"
]
for col in columns:
    suite.add_expectation(gx.expectations.ExpectColumnToExist(column=col))

# Colonnes non-nulles
columns_not_null = [
    "id_employe", "nom", "prenom", "date_naissance", "date_embauche", 
    "salaire_brut", "type_contrat", "nb_jours_cp","mode_deplacement"
]
for col in columns_not_null:
    suite.add_expectation(gx.expectations.ExpectColumnValuesToNotBeNull(column=col))

# Unicité de l'ID
suite.add_expectation(gx.expectations.ExpectColumnValuesToBeUnique(column="id_employe"))

# Dates plausibles
suite.add_expectation(gx.expectations.ExpectColumnPairValuesAToBeGreaterThanB(column_A="date_embauche", column_B="date_naissance", or_equal=False))

# Modes de déplacement
modes_deplacement = [
    "Transports en commun",
    "véhicule thermique/électrique",
    "Marche/running",
    "Vélo/Trottinette/Autres"
    ]

suite.add_expectation(gx.expectations.ExpectColumnValuesToBeInSet(column="mode_deplacement", value_set=modes_deplacement))

# Prénoms et noms alphabétiques
suite.add_expectation(gx.expectations.ExpectColumnValuesToMatchRegex(column="prenom", regex="^[A-Za-zÀ-ÿ' -]+$"))
suite.add_expectation(gx.expectations.ExpectColumnValuesToMatchRegex(column="nom", regex="^[A-Za-zÀ-ÿ' -]+$"))
