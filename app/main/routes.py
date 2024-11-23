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
    ban_data, description = db_service.fetch_all("ban")
    ban_df = pd.DataFrame(ban_data, columns=description)
    ban_df.to_csv("ban.csv", index=False)
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
    
            df_client = pd.read_excel(file)
        elif file_extension == '.csv':
       
            file_content = file.read().decode('utf-8')  
            file.seek(0)  
            sniffer = csv.Sniffer()
            delimiter = sniffer.sniff(file_content).delimiter

    
            df_client = pd.read_csv(file, delimiter=delimiter)
        else:
            return jsonify({'message': 'Type de fichier non supporté'}), 400
        
        df_client["nom_voie"] = df_client["nom_voie"].apply(normalize_text)
        
        df_client["cp_no_voie"] = delExcessBlanc(df_client["cp_no_voie"])
        df_client["cp_no_voie"] = delPoint(df_client["cp_no_voie"])
        df_client["cp_no_voie"] = df_client["cp_no_voie"].apply(normalize_text)

        df_client["nom_voie"] = delExcessBlanc(df_client["nom_voie"])
        df_client["nom_voie"] = delPoint(df_client["nom_voie"])
        df_client["nom_voie"] = delArticle(df_client["nom_voie"])

        df_client["type_voie"] = df_client["type_voie"].apply(normalize_text)
        df_client["type_voie"] = delExcessBlanc(df_client["type_voie"])
        df_client["nom_voie"] = delPoint(df_client["nom_voie"])
        
        ban_data, description = db_service.fetch_all("ban")
        df_ban = pd.DataFrame(ban_data, columns=description)


        df_client["id"] = df_client.index

        seuil = 0.8
        nb_corrige = 0
        nb_total = len(df_client)
        nb_no_match = 0
        type_voie = pd.DataFrame(df_ban['type_voie'].unique(), columns=['type_voie'])
        df_client = pd.merge(df_client, type_voie, left_on='type_voie',
                            right_on='type_voie', how='left', indicator=True)
        left_only = df_client[df_client['_merge'] == 'left_only']
        df_client.drop(columns=['_merge'], inplace=True, errors='ignore')
        dist = distLevenshtein(df_ban['type_voie'].unique(), left_only['type_voie'])
        df_client = pd.merge(df_client, dist.add_prefix("tv_"), left_on='type_voie',
                            right_on='tv_input_address', how='left')
        nom_voie = pd.DataFrame(df_ban['nom_voie'].unique(), columns=['nom_voie'])
        df_client = pd.merge(df_client, nom_voie, left_on='nom_voie',
                            right_on='nom_voie', how='left', indicator=True)
        left_only = df_client[df_client['_merge'] == 'left_only']
        df_client.drop(columns=['_merge'], inplace=True, errors='ignore')
        dist = distLevenshtein(df_ban['nom_voie'].unique(), left_only['nom_voie'])
        df_client = pd.merge(df_client, dist.add_prefix(
            "nv_"), left_on='nom_voie', right_on='nv_input_address', how='left')


        df_client = df_client.drop_duplicates(subset='id', keep='first')
        nb_no_match = df_client[df_client['tv_match_confidence'] < seuil].shape[0] + \
            df_client[df_client['nv_match_confidence'] < seuil].shape[0]
        nb_corrige = df_client[df_client['tv_match_confidence'] > seuil].shape[0] + \
            df_client[df_client['nv_match_confidence'] > seuil].shape[0]
        pourcent_no_match = (nb_no_match * 100) / nb_total
        pourcent_corrige = (nb_corrige * 100) / nb_total

        pourcent_correct = 100 - pourcent_no_match - pourcent_corrige
        
        summary = {
            "correct_pourcent": pourcent_correct,
            "corriger_pourcent": pourcent_corrige,
            "no_match_pourcent": pourcent_no_match
        }
        
        df_no_match_corrige = df_client['tv_match_confidence'].notna() | df_client['nv_match_confidence'].notna()
        
        df_no_match_corrige['tv_best_match'] = df_no_match_corrige['tv_best_match'].fillna(
            df_no_match_corrige['type_voie'])


        df_no_match_corrige['nv_best_match'] = df_no_match_corrige['nv_best_match'].fillna(
            df_no_match_corrige['nom_voie'])
        df_no_match_corrige[['num_voie', 'type_voie', 'nom_voie', 'tv_best_match', 'nv_best_match']] = df_no_match_corrige[
            ['num_voie', 'type_voie', 'nom_voie', 'tv_best_match', 'nv_best_match']
        ].fillna('')

        df_no_match_corrige['num_voie'] = df_no_match_corrige['num_voie'].astype(str)
        df_no_match_corrige['type_voie'] = df_no_match_corrige['type_voie'].astype(str)
        df_no_match_corrige['nom_voie'] = df_no_match_corrige['nom_voie'].astype(str)
        df_no_match_corrige['tv_best_match'] = df_no_match_corrige['tv_best_match'].astype(
            str)
        df_no_match_corrige['nv_best_match'] = df_no_match_corrige['nv_best_match'].astype(
            str)


        df_no_match_corrige['adresse_origine'] = df_no_match_corrige['num_voie'] + ' ' + \
            df_no_match_corrige['type_voie'] + ' ' + df_no_match_corrige['nom_voie']


        df_no_match_corrige['adress_corrigé'] = df_no_match_corrige['num_voie'].astype(str) + ' ' + \
            df_no_match_corrige['tv_best_match'].astype(str) + ' ' + \
            df_no_match_corrige['nv_best_match'].astype(str)
            
        df_client['correct_address'] = (
            df_client['num_voie'].astype(str) + " " +
            df_client['cp_no_voie'].astype(str) + " " +
            df_client['type_voie'].astype(str) + " " +
            df_client['nom_voie'].astype(str)
        )

        map_df = df_client[['long', 'lat', 'correct_address']].dropna(
            subset=['long', 'lat', 'correct_address'])

        map_json = map_df.to_json(orient='records', force_ascii=False)
        df_no_match_corrige['average_fiability'] = df_no_match_corrige.apply(
            lambda row: (row['nv_match_confidence'] +
                         row['tv_match_confidence']) / 2
            if pd.notnull(row['nv_match_confidence']) and pd.notnull(row['tv_match_confidence'])
            else row['nv_match_confidence'] if pd.notnull(row['nv_match_confidence'])
            else row['tv_match_confidence'], axis=1
        )
        df_no_match_corrige = df_no_match_corrige[['adresse_origine', 'adress_corrigé', 'average_fiability']]
        df_no_match_corrige_json = df_no_match_corrige.to_json(
            orient='records', force_ascii=False)
        
  



        df_client = df_client[['num_voie', 'cp_no_voie', 'type_voie', 'nom_voie', 'code_poste','nom_com']].copy()
       
        df_json = df_client.to_json(orient='records', force_ascii=False)
      

        return jsonify({
            "export": df_json,
            "stats": summary,
            "map": map_json,
            "tab": df_no_match_corrige_json
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
        df["rep"] = df["rep"].apply(normalize_text)
        df["rep"] = delExcessBlanc(df["rep"])

        
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



