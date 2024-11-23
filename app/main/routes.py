from flask import Blueprint, render_template, redirect, session, url_for, send_file, request, jsonify
from db.db_service import DatabaseService
from .services import *
import pandas as pd
import numpy as np
import gzip
import shutil
import os
import csv
load_dotenv()


main_bp = Blueprint('main', __name__, template_folder='templates',
                    static_folder='static', static_url_path='/main/static')
db_service = DatabaseService()


@main_bp.route('/')
def redirection():
    return redirect(url_for('main.page_main'))


@main_bp.route('/bonne_adresse/')
def page_main():
    return render_template('main.html')


@main_bp.route('/bonne_adresse/upload_file/', methods=['POST'])
def upload_file():
    if 'file' not in request.files:
        return jsonify({'message': 'Aucun fichier'}), 400

    file = request.files['file']

    if file.filename == '':
        return jsonify({'message': 'Aucun fichier sélectionné'}), 400

    file_extension = os.path.splitext(file.filename)[1].lower()

    try:
        if file_extension in ['.xls', '.xlsx']:
    
            df = pd.read_excel(file)
        elif file_extension == '.csv':
       
            file_content = file.read().decode('utf-8')  
            file.seek(0)  
            sniffer = csv.Sniffer()
            delimiter = sniffer.sniff(file_content).delimiter

    
            df = pd.read_csv(file, delimiter=delimiter)
        else:
            return jsonify({'message': 'Type de fichier non supporté'}), 400

        df['adress'] = (
            df['num_voie'].astype(str) + " " +
            df['cp_no_voie'].astype(str) + " " +
            df['type_voie'].astype(str) + " " +
            df['nom_voie'].astype(str)
        )
        map_df = df[['x', 'y', 'adress']]
        map_df.rename(columns={'x': 'lon', 'y': 'lat'}, inplace=True)
        summary = {
            "correct_pourcent": 65,
            "corriger_pourcent": 30,
            "no_match_pourcent": 5  
        }
        tab = {
            "correct_address": "Lotissement Chemin des Barrières",
            "origine_address": "Lotissement Chemin des Baarrières",
            "fiability": "90%"
        }

    
        df_json = df.to_json(orient='records', force_ascii=False)
        map_json = map_df.to_json(orient='records', force_ascii=False)

        return jsonify({
            "export": df_json,
            "stats": summary,
            "map": map_json,
            "tab": tab
        }), 200

    except Exception as e:
        
        return jsonify({'message': f'Erreur lors du traitement du fichier : {str(e)}'}), 500


@main_bp.route('/bonne_adresse/update_ban/', methods=['GET'])
def update_ban():
    try:
        url = "https://adresse.data.gouv.fr/data/ban/adresses/latest/csv/adresses-974.csv.gz"
        destination_dir = "data"  # Répertoire de sortie
        download_file(url, destination_dir)
        # Chemin vers le fichier .gz
        chemin_fichier_gz = "data/adresses-974.csv.gz"
        # Optionnel : Spécifie un fichier de sortie
        chemin_fichier_sortie = "data/res"
        # Appelle la fonction
        df = pd.read_csv(decompresser_gz(chemin_fichier_gz,
                         chemin_fichier_sortie), sep=";", encoding="utf-8")
        
        df["nom_voie"] = df["nom_voie"].apply(normalize_text)

        
        df["nom_voie"] = delExcessBlanc(df["nom_voie"])
        
        df["nom_voie"] = delParenthese(df["nom_voie"])
        df["nom_voie"] = delQuotes(df["nom_voie"])

        df = extract(df)
        df["nom_voie"] = delArticle(df["nom_voie"])
        df["nom_voie"] = replHyphens(df["nom_voie"])
        df.rename(columns={'id': 'id_ban',
                  'numero': 'num_voie', 'lon': 'long'}, inplace=True)
        df = df[['id_ban', 'num_voie', 'rep', 'type_voie',
                 'nom_voie', 'code_postal', 'lat', 'long']].copy()
  
        insert_data(df)
        return f"ok", 200
    except Exception as e:
        return f"Erreur: {e}", 500


def transform_df(df):

    return