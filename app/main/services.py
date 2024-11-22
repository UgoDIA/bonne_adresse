import re
import gzip
import shutil
import os
import urllib.request
import unicodedata
from sqlalchemy import create_engine
from dotenv import load_dotenv
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

# - Extrait et Supprime les types de voie


def extract(df):
    mots = r"LOTISSEMENT|CHEMIN|RUE|ALLEE|BOULEVARD|AVENUE|IMPASSE|RUELLE|ROUTE(?!\s+(?:DEPARTEMENTALE|NATION))"
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
    connection_string = f"postgresql://{dbuser}:{dbpwd}@{dbhost}:{dbport}/{dbname}"
 
        # Créer une connexion à la base de donnéess
    engine = create_engine(connection_string)

    # Insérer les données dans la table 
    df.to_sql('ban', con=engine, if_exists='replace', index=False)
    print("Données insérées avec succès dans la base de données.")

