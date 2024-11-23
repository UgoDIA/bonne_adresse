import re
import gzip
import shutil
import os
import urllib.request
import unicodedata
from sqlalchemy import create_engine
from dotenv import load_dotenv
import pandas as pd
from Levenshtein import distance
load_dotenv()


def decompresser_gz(chemin_fichier_gz, repertoire_sortie=None):
    """
    Décompresse un fichier .gz dans un répertoire spécifique

    Args:
        chemin_fichier_gz (str): Chemin vers le fichier .gz à décompresser
        repertoire_sortie (str, optional): Répertoire où sauvegarder le fichier décompressé.
                                         Si non spécifié, utilise le même répertoire que le fichier source

    Returns:
        str: Chemin complet du fichier décompressé
    """
    try:
        # Récupérer le nom du fichier sans le .gz
        nom_fichier = os.path.basename(chemin_fichier_gz)
        if nom_fichier.endswith('.gz'):
            nom_fichier = nom_fichier[:-3]

        # Si aucun répertoire de sortie n'est spécifié, utiliser le répertoire du fichier source
        if repertoire_sortie is None:
            repertoire_sortie = os.path.dirname(chemin_fichier_gz)

        # Créer le répertoire de sortie s'il n'existe pas
        os.makedirs(repertoire_sortie, exist_ok=True)

        # Construire le chemin complet de sortie
        chemin_sortie = os.path.join(repertoire_sortie, nom_fichier)

        # Décompression du fichier
        with gzip.open(chemin_fichier_gz, 'rb') as f_in:
            with open(chemin_sortie, 'wb') as f_out:
                shutil.copyfileobj(f_in, f_out)

        return chemin_sortie

    except Exception as e:
        print(f"Erreur lors de la décompression : {str(e)}")
        return None


def download_file(url, destination_dir):
    try:
        # Extraction du nom du fichier depuis l'URL
        file_name = os.path.basename(url)
        # Construction du chemin complet du fichier
        destination_path = os.path.join(destination_dir, file_name)

        # Téléchargement du fichier
        urllib.request.urlretrieve(url, destination_path)
        print(f"Fichier téléchargé avec succès : {destination_path}")
    except Exception as e:
        print(f"Erreur lors du téléchargement : {e}")
    return destination_path


def normalize_text(text):
    if isinstance(text, str):  # Vérifie que c'est une chaîne
        # Convertit en majuscules
        text_upper = text.upper()
        # Supprime les accents
        text_normalized = unicodedata.normalize('NFD', text_upper)
        text_without_accents = ''.join(
            c for c in text_normalized if unicodedata.category(c) != 'Mn')
        return text_without_accents
    return text  # Retourne tel quel si ce n'est pas une chaîne

# - Supprime les espaces blanc en trop ------------------------------------------------------------------------


def delExcessBlanc(vect):
    return (vect.str.replace(r"\s+", " ", regex=True).str.strip())

# - Supprime ce qu'il y a entre paenthèse ainsi que les parenthèse --------------------------------------------


def delParenthese(vect):
    return vect.str.replace(r"\(.*?\)", "", regex=True)


def delQuotes(vect):
    return vect.str.replace(r'"', '', regex=True)


def replHyphens(vect):
    return vect.str.replace(r'-', ' ', regex=True)


def delPoint(vect):
    return vect.str.replace(r'\.', '', regex=True)

# - Extrait et Supprime les types de voie


def extract(df):
    mots = r"LOTISSEMENT|CHEMIN|RUE|SENTIER|ALLEE|BOULEVARD|AVENUE|IMPASSE|RUELLE|ROUTE(?!\s+(?:DEPARTEMENTALE|NATION))"
    # Extraire le type de voie
    df['type_voie'] = df['nom_voie'].str.extract(rf"^({mots})\b", expand=False)
    # Supprimer le type de voie extrait du champ `nom_voie`
    df['nom_voie'] = df['nom_voie'].str.replace(
        rf"^({mots})\b\s*", "", regex=True).str.strip()
    return df


# - Suppresion de l'article -----------------------------------------------------------------------------------


def delArticle(vect):
    articles = r"DE L[’'\s]|DE LA |DES |DU |LE |LA |L[’'\s]|DE "
    # Suppression de tous les articles au début, y compris L'
    return vect.str.replace(rf"^({articles})\s*", "", regex=True, flags=re.IGNORECASE).str.strip()


def insert_data(df):
    # Charge les variables d'environnement

    dbname = os.environ.get('DBNAME')
    dbuser = os.environ.get('DBUSER')
    dbpwd = os.environ.get('DBPWD')
    dbhost = os.environ.get('DBHOST')
    dbport = os.environ.get('DBPORT')

    #  chaîne de connexion PostgreSQL
    connection_string = f"postgresql://{dbuser}:{
        dbpwd}@{dbhost}:{dbport}/{dbname}"

    # Créer une connexion à la base de donnéess
    engine = create_engine(connection_string)

    # Insérer les données dans la table
    df.to_sql('ban', con=engine, if_exists='replace', index=False)
    print("Données insérées avec succès dans la base de données.")

    def replace_with_full_name(df, abbreviations_dict, column='A'):
        """
        Remplace les abréviations dans une colonne d'un DataFrame par leur forme complète
        uniquement lorsqu'elles constituent un mot entier.

        Parameters:
        -----------
        df : pandas.DataFrame
            Le DataFrame contenant les données à modifier
        abbreviations_dict : dict
            Dictionnaire avec les formes complètes comme clés et les listes d'abréviations comme valeurs
        column : str, optional (default='A')
            Nom de la colonne à modifier

        Returns:
        --------
        pandas.DataFrame
            DataFrame avec les valeurs remplacées
        """
        # Créer une copie du DataFrame pour ne pas modifier l'original
        df_copy = df.copy()

        # Créer un dictionnaire inversé pour mapper les abréviations vers leur forme complète
        inverse_dict = {}
        for full_name, abbreviations in abbreviations_dict.items():
            for abbr in abbreviations:
                inverse_dict[abbr.upper()] = full_name
                # Ajouter aussi la forme complète comme clé pour gérer le cas où elle est déjà présente
                inverse_dict[full_name.upper()] = full_name

        def replace_word(text):
            """
            Remplace uniquement les mots entiers qui correspondent aux abréviations
            """
            if pd.isna(text):
                return text

            words = text.upper().split()
            new_words = []

            for word in words:
                # Vérifier si le mot entier correspond à une abréviation
                if word in inverse_dict:
                    new_words.append(inverse_dict[word])
                else:
                    new_words.append(word)

            return ' '.join(new_words)

        # Appliquer la fonction de remplacement
        df_copy[column] = df_copy[column].apply(replace_word)

        return df_copy

    abreviations = {
        "CHEMIN": ["CH", "CHE"],
        "RUE": ["R", "RUE"],
        "ALLEE": ["ALL", "A"],
        "BOULEVARD": ["BD", "BLV"],
        "AVENUE": ["AV", "AVE"],
        "IMPASSE": ["IMP", "IPS"],
        "RUELLE": ["RL", "RLE"],
        "ROUTE": ["RT", "RO"],
        "CHEMIN DEPARTEMENTAL": ["CD", "CH DEP", "CHD"],
        "ROUTE DEPARTEMENTALE": ["RD", "RDEP"],
        "ROUTE NATIONALE": ["RN", "RNAT"],
        "LOTISSEMENT": ["LOT", "LTMT", "LTSM"]
    }


def distLevenshtein(reference_addresses, input_addresses):
    """
    Compare chaque adresse d'entrée avec toutes les adresses de référence
    et trouve la meilleure correspondance.

    Parameters:
    -----------
    reference_addresses : pandas.Series
        Série contenant les adresses de référence
    input_addresses : pandas.Series
        Série contenant les adresses à vérifier

    Returns:
    --------
    pandas.DataFrame
        DataFrame contenant les adresses d'entrée, leurs meilleures correspondances
        et les scores de similarité
    """
    # Liste pour stocker les résultats
    results = []

    # Pour chaque adresse d'entrée
    for input_addr in input_addresses:
        best_match = None
        min_distance = float('inf')

        # Comparaison avec chaque adresse de référence
        for ref_addr in reference_addresses:
            dist = distance(str(input_addr).lower(), str(ref_addr).lower())

            if dist < min_distance:
                min_distance = dist
                best_match = ref_addr

        # Ajouter le résultat à la liste
        results.append({
            'input_address': input_addr,
            'best_match': best_match,
            'similarity_score': min_distance,
            'match_confidence': 1 - (min_distance / max(len(str(input_addr)), len(str(best_match))))
        })

    # Créer un DataFrame avec les résultats
    results_df = pd.DataFrame(results)

    # Trier par score de confiance
    results_df = results_df.sort_values('match_confidence', ascending=False)

    return results_df


def coalesce(*columns):
    df = pd.concat(columns, axis=1)
    return df.bfill(axis=1).iloc[:, 0]
