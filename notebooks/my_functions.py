import matplotlib.pyplot as plt
import seaborn as sns
import pandas as pd
import numpy as np
from pandas.api.types import CategoricalDtype
from scipy.stats import shapiro, chi2_contingency, f_oneway, pearsonr, spearmanr, kruskal
from itertools import combinations

# === Installation des dépendances avec Poetry :
# poetry add matplotlib seaborn pandas numpy scipy

# my_functions.calculate_nan(df) : Calcule le nombre et % de NaN dans un df par colonne
# my_functions.identify_column_types(df) : Identifie le type de chaque colonne d'un dataframe : Date - Binaire - Catégorielle - Continue - Inconue
# my_functions.plot_column_analysis(df, column_types) : Pour chaque colonne d'un dataframe créé une représentation visuelle
# my_functions.analyze_correlations(df) : retourne un tableau contenant les colonnes comparées, les p-values et les coefficients de corrélation.


def calculate_nan(df):
    '''
    Calcule le nombre et % de NaN dans un df par colonne
    '''
    # Compte les valeurs None
    nan_counts = df.isnull().sum()

    # Compte les valeurs string "NaN", "nan", "none"
    str_nan_counts = df.isin(["NaN", "nan", "none"]).sum()

    # Compte les valeurs vides
    empty_counts = (df == "").sum()

    # Compte les valeurs contenant uniquement des espaces
    space_counts = {}
    for col in (df.select_dtypes(include='object')).columns : 
        space_counts[col] = df[col].str.isspace().sum()
    space_counts = pd.Series(space_counts)

    # Création du DataFrame de toutes les données NaN
    merged_nan_counts = pd.DataFrame({
        'NaN counts': nan_counts.reindex(df.columns, fill_value=0),
        'Str nan counts': str_nan_counts.reindex(df.columns, fill_value=0),
        'Empty counts': empty_counts.reindex(df.columns, fill_value=0),
        'Space counts': space_counts.reindex(df.columns, fill_value=0),
    })

    # Ajout d'une colonne total de NaN
    merged_nan_counts['Total NaN'] = merged_nan_counts.sum(axis=1)

    # Ajout % de NaN
    merged_nan_counts['% NaN'] = round(merged_nan_counts['Total NaN'] / len(df) * 100, 2)

    # Retourner le DataFrame
    return merged_nan_counts


def identify_column_types(df):
    '''
    Identifie le type de chaque colonne d'un dataframe : Date - Binaire - Catégorielle - Continue - Inconue
    Une colonne numérique ayant moins de 10 valeurs uniques est considérée comme catégorielle
    '''
    
    column_types = pd.DataFrame({
    'column_name': df.columns,
    'c_type': ['' for _ in df.columns]
    })

    for col in df.columns:
        # Vérifier si la colonne est de type datetime
        if pd.api.types.is_datetime64_any_dtype(df[col]):
            column_types.loc[column_types["column_name"] == col, "c_type"] = 'Date'
        # Vérifier si la colonne est binaire
        elif df[col].nunique() == 2:
            column_types.loc[column_types["column_name"] == col, "c_type"] = 'Binaire'
        # Vérifier si la colonne est catégorielle (objet ou catégorie)
        elif df[col].dtype == 'object' or isinstance(df[col].dtype, CategoricalDtype):
            column_types.loc[column_types["column_name"] == col, "c_type"] = 'Catégorielle'
        # Vérifier si la colonne est numérique mais avec moins de 10 valeurs distinctes
        elif np.issubdtype(df[col].dtype, np.number) and df[col].nunique() < 10:
            column_types.loc[column_types["column_name"] == col, "c_type"] = 'Catégorielle'
        # Vérifier si la colonne est continue (numérique)
        elif np.issubdtype(df[col].dtype, np.number):
            column_types.loc[column_types["column_name"] == col, "c_type"] = 'Continue'
        else:
            column_types.loc[column_types["column_name"] == col, "c_type"] = 'Inconnu'
    
    return column_types


def plot_column_analysis(df, column_types):
    '''
    Pour chaque colonne d'un dataframe créé une représentation visuelle : 
    - colonne continue : boxplot
    - colonne catégorielle : barplot
    '''

    # Créer des graphiques pour les colonnes continues et catégorielles
    for col in df.columns:
        # Si la colonne est continue (numérique)
        if column_types.loc[column_types["column_name"] == col, "c_type"].values[0] == "Continue":
            plt.figure(figsize=(8, 6))
            sns.boxplot(x=df[col])
            plt.title(f'Boxplot pour la colonne {col}')
            
            # Calcul des statistiques
            mean = df[col].mean()
            median = df[col].median()
            std_dev = df[col].std()
            
            # Affichage des statistiques sur le graphique
            plt.figtext(0.15, 0.85, f'Moyenne: {mean:.2f}', fontsize=12)
            plt.figtext(0.15, 0.80, f'Médiane: {median:.2f}', fontsize=12)
            plt.figtext(0.15, 0.75, f'Écart-type: {std_dev:.2f}', fontsize=12)
            
            plt.show()

        # Si la colonne est catégorielle
        elif column_types.loc[column_types["column_name"] == col, "c_type"].values[0] in ["Catégorielle", "Binaire"]:
            plt.figure(figsize=(8, 6))
            value_counts = df[col].value_counts()
            value_percent = df[col].value_counts(normalize=True) * 100
            nan_percent = df[col].isna().sum() / len(df) * 100
            
            # Création du bar plot
            sns.barplot(x=value_counts.index, y=value_counts.values)
            plt.title(f'Bar Plot pour la colonne {col}')
            plt.ylabel('Fréquence')
            
            # Affichage des pourcentages sur le graphique
            for patch, (category, percent) in zip(plt.gca().patches, value_percent.items()):
                plt.text(patch.get_x() + patch.get_width() / 2., patch.get_height() + 1, f'{percent:.2f}%', ha='center', fontsize=10)

            # Afficher les NaN
            plt.figtext(0.15, 0.85, f'% NaN: {nan_percent:.2f}%', fontsize=12)
            plt.show()



def analyze_correlations(df):
    '''
    Analyse les corrélations entre les colonnes d'un DataFrame en fonction de leur type (continue ou catégorielle).
    Applique des tests statistiques appropriés (Khi-deux, ANOVA, Pearson ou Spearman, Kruskal-Wallis)
    et retourne un tableau contenant les colonnes comparées, les p-values, les coefficients de corrélation et l’interprétation.
    '''

    # Fonction interne d'interprétation des p-values
    def interpret_p_value(p):
        if p is None:
            return "Test non applicable"
        elif p > 0.05:
            return "Aucune relation significative"
        elif p > 0.01:
            return "Significatif"
        elif p > 0.001:
            return "Très significatif"
        else:
            return "Hautement significatif"

    # Supprimer les lignes contenant des NaN dans le DataFrame
    df_cleaned = df.dropna()

    # Détection des types de colonnes
    column_types = []
    for col in df_cleaned.columns:
        if np.issubdtype(df_cleaned[col].dtype, np.number):
            if df_cleaned[col].nunique() > 10:
                column_types.append((col, 'continue'))
            else:
                column_types.append((col, 'categorielle'))
        else:
            column_types.append((col, 'categorielle'))

    # Préparation du tableau final
    results = []

    # Boucle sur les combinaisons de colonnes
    for col1, col2 in combinations(df_cleaned.columns, 2):
        type1 = next(ct[1] for ct in column_types if ct[0] == col1)
        type2 = next(ct[1] for ct in column_types if ct[0] == col2)

        if type1 == 'categorielle' and type2 == 'categorielle':
            contingency_table = pd.crosstab(df_cleaned[col1], df_cleaned[col2])
            if contingency_table.size == 0:
                results.append((col1, col2, None, None, "Table de contingence vide"))
                continue
            chi2, p, _, _ = chi2_contingency(contingency_table)
            results.append((col1, col2, p, None, interpret_p_value(p)))

        elif (type1 == 'categorielle' and type2 == 'continue') or (type1 == 'continue' and type2 == 'categorielle'):
            if type1 == 'categorielle':
                groups = [df_cleaned[col2][df_cleaned[col1] == category] for category in df_cleaned[col1].unique()]
            else:
                groups = [df_cleaned[col1][df_cleaned[col2] == category] for category in df_cleaned[col2].unique()]

            valid_groups = [group for group in groups if len(group) > 0]
            if len(valid_groups) >= 2:
                normality_check = [shapiro(group)[1] > 0.05 for group in valid_groups]
                if all(normality_check):
                    f_stat, p = f_oneway(*valid_groups)
                else:
                    h_stat, p = kruskal(*valid_groups)
                results.append((col1, col2, p, None, interpret_p_value(p)))
            else:
                results.append((col1, col2, None, None, "Groupes insuffisants"))

        elif type1 == 'continue' and type2 == 'continue':
            normal1 = shapiro(df_cleaned[col1])[1] > 0.05
            normal2 = shapiro(df_cleaned[col2])[1] > 0.05

            if normal1 and normal2:
                corr, p = pearsonr(df_cleaned[col1], df_cleaned[col2])
            else:
                corr, p = spearmanr(df_cleaned[col1], df_cleaned[col2])

            if np.isnan(corr):
                corr = None

            results.append((col1, col2, p, corr, interpret_p_value(p)))

    # Conversion en DataFrame final
    results_df = pd.DataFrame(results, columns=['Colonne 1', 'Colonne 2', 'p-value', 'Coefficient de corrélation', 'Interprétation'])
    return results_df

def plot_multiple_x_y_relationships(X, y, column_types):

    '''
    Trace des représentations graphiques pour chaque colonne de X et y :
    - Si une colonne de X est continue, un nuage de points avec régression linéaire.
    - Si une colonne de X est catégorielle, un boxplot pour y selon les catégories de X.
    
    Args:
    X : DataFrame contenant les variables indépendantes (colonnes X).
    y : DataFrame ou Series contenant la variable dépendante (y).
    column_types : DataFrame contenant les types de chaque colonne (résultat de la fonction 'identify_column_types').
    '''
    
    # Parcourir chaque colonne de X et créer le graphique
    for col in X.columns:
        # Vérifier le type de la colonne
        x_type = column_types.loc[column_types["column_name"] == col, "c_type"].values[0]
        
        if x_type == "Continue":
            # Si la colonne de X est continue, faire un nuage de points avec régression linéaire
            plt.figure(figsize=(12, 6))
            sns.regplot(x=col, y=y, data=X, scatter_kws={'s': 30}, line_kws={"color": "red"})

            # Calcul des statistiques pour X (moyenne, médiane, écart-type) et y
            x_mean = X[col].mean()
            x_median = X[col].median()
            x_std_dev = X[col].std()
            
            y_mean = y.mean()
            y_median = y.median()
            y_std_dev = y.std()

            # Position de départ pour afficher les informations sur la droite
            text_x_position = 1.05  # Décalage à droite du graphique

            # Affichage des statistiques sur la droite du graphique
            plt.text(text_x_position, 0.85, f'Moyenne X: {x_mean:.2f}', fontsize=12, ha='left', transform=plt.gca().transAxes)
            plt.text(text_x_position, 0.80, f'Médiane X: {x_median:.2f}', fontsize=12, ha='left', transform=plt.gca().transAxes)
            plt.text(text_x_position, 0.75, f'Écart-type X: {x_std_dev:.2f}', fontsize=12, ha='left', transform=plt.gca().transAxes)
            plt.text(text_x_position, 0.70, f'Moyenne Y: {y_mean:.2f}', fontsize=12, ha='left', transform=plt.gca().transAxes)
            plt.text(text_x_position, 0.65, f'Médiane Y: {y_median:.2f}', fontsize=12, ha='left', transform=plt.gca().transAxes)
            plt.text(text_x_position, 0.60, f'Écart-type Y: {y_std_dev:.2f}', fontsize=12, ha='left', transform=plt.gca().transAxes)

            plt.title(f'Nuage de points avec régression linéaire entre {col} et la variable cible')
            plt.show()
        
        elif x_type == "Catégorielle" or x_type == "Binaire":
            # Si la colonne de X est catégorielle ou binaire, faire un boxplot pour y
            plt.figure(figsize=(12, 6))
        

            sns.boxplot(x=col, y=y, data=X)
            plt.title(f'Boxplot de {y.name} par catégories de {col}')
            
            # Calcul des statistiques pour chaque catégorie de la colonne X
            value_counts = X[col].value_counts()
            value_percent = X[col].value_counts(normalize=True) * 100
            nan_percent = X[col].isna().sum() / len(X) * 100
            
            # Position de départ pour afficher les informations sur la droite
            text_x_position = 1.05  # Décalage à droite du graphique
            
            # Afficher les pourcentages et les valeurs pour chaque catégorie
            for i, category in enumerate(value_counts.index):
                plt.text(text_x_position, 0.95 - i * 0.1, 
                         f'{category}: {value_counts[category]} ({value_percent[category]:.2f}%)', 
                         fontsize=10, ha='left', transform=plt.gca().transAxes)
            
            # Affichage du pourcentage de NaN à droite du graphique
            plt.text(text_x_position, 0.95 - (len(value_counts) + 1) * 0.1, 
                     f'% NaN: {nan_percent:.2f}%', fontsize=12, ha='left', transform=plt.gca().transAxes)
            
            plt.show()
        
        else:
            print(f"Le type de la colonne {col} est inconnu ou non pris en charge pour cette analyse.")

def plot_correlation_matrix(df, sample_size=5000, alpha=0.05):
    """
    Affiche une matrice de corrélation pour les colonnes numériques continues du DataFrame.
    
    Paramètres :
    - df : DataFrame pandas contenant les données
    - sample_size : Nombre max d'échantillons pour le test de normalité (défaut 5000)
    - alpha : Niveau de significativité pour le test de Shapiro-Wilk (défaut 0.05)
    
    La méthode de corrélation est choisie en fonction de la normalité des colonnes :
    - Pearson si au moins 50% des colonnes sont normales
    - Spearman sinon
    """
    
    # Sélectionner les colonnes numériques avec plus de 10 valeurs uniques
    num_cols = [col for col in df.select_dtypes(include=[np.number]).columns if df[col].nunique() > 10]
    
    if not num_cols:
        print("Aucune colonne continue trouvée.")
        return
    
    # Tester la normalité avec Shapiro-Wilk
    normal_cols = []
    non_normal_cols = []

    for col in num_cols:
        sample = df[col].sample(min(sample_size, len(df)), random_state=42)  # Échantillon si nécessaire
        stat, p = shapiro(sample)
        if p > alpha:
            normal_cols.append(col)
        else:
            non_normal_cols.append(col)

    # Choisir la méthode de corrélation
    method = "pearson" if len(non_normal_cols) / len(num_cols) <= 0.5 else "spearman"

    # Calcul de la matrice de corrélation
    corr_matrix = df[num_cols].corr(method=method)

    # Affichage avec heatmap
    plt.figure(figsize=(10, 8))
    sns.heatmap(corr_matrix, annot=True, cmap="coolwarm", fmt=".2f", linewidths=0.5)
    plt.title(f"Matrice de corrélation ({method.capitalize()})")
    plt.show()