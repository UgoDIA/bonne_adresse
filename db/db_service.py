from flask import jsonify, request
from db.db import DatabasePool
import os
from sql.requetes import queries
from datetime import time
import numpy as np
from psycopg2.extras import execute_values
import pandas as pd


class DatabaseService:
    def __init__(self):
        self.db_pool = DatabasePool()


    def get_connection(self):
        return self.db_pool.get_connection()

    def put_connection(self, conn):
        self.db_pool.put_connection(conn)

    def close_pool(self):
        self.db_pool.close_pool()

    # sérialise un objet de type time en chaîne de caractères
    def serialize_time(self, t):
        if isinstance(t, time):
            return t.strftime('%H:%M:%S')
        return t

    # sérialise un objet 
    def serialize_row(self, row, description):
        if row is None:
            return None
        row_dict = dict(zip([desc[0] for desc in description], row))
        for key, value in row_dict.items():
            if isinstance(value, time):
                row_dict[key] = self.serialize_time(value)
        return row_dict

    # récupére toutes les lignes d'une table
    def fetch_all(self, table_name, order_by=None, order='asc', filters=None):
        conn = self.get_connection()
        params = []
        try:
            with conn.cursor() as cur:
                #debut de la requête
                query = f'SELECT * FROM {table_name}'
                # query = f'SELECT id_prochaine_etape, nom_etape, type_affaire, libelle_etape, condition FROM {table_name}'

                # recupère les filtres s'il y en a
                if filters:
                    filter_clauses = []
                    
                    
                    for key, value in filters.items():
                        filter_clauses.append(f"{key} = %s")
                        params.append(value)
                    
                    # ajoute le where
                    query += ' WHERE ' + ' AND '.join(filter_clauses)
                
                # ajoute le order by s'il y en a
                if order_by:
                    query += f' ORDER BY {order_by} {order.upper()}'
                
                # execute la requête
                cur.execute(query, tuple(params))
                results = cur.fetchall()
                description = cur.description
        finally:
            self.put_connection(conn)
            
        return results, description

    # récupére une ligne spécifique par son ID
    def fetch_by_id(self, table_name, item_id):
        conn = self.get_connection()
        try:
            with conn.cursor() as cur:
                id_column_name = f"id_{table_name.lower()}"
                cur.execute(f'SELECT * FROM {table_name} WHERE {id_column_name} = %s;', (item_id,))
                result = cur.fetchone()
                description = cur.description
        
        finally:
            self.put_connection(conn)
        return result, description

    # créer une nouvelle ligne
    def create_item(self, table_name, columns, values):
        conn = self.get_connection()
        try:
            with conn.cursor() as cur:
                id_column_name = f"id_{table_name.lower()}"
                values = [None if pd.isna(value) or value in ['', 'NaT'] else value for value in values]
                
                if not values:
                    cur.execute(f'''
                        INSERT INTO {table_name} DEFAULT VALUES RETURNING {id_column_name};
                    ''')
                else:
                    placeholders = ', '.join(['%s'] * len(values))
                    columns_str = ', '.join(columns)
                    cur.execute(f'''
                        INSERT INTO {table_name} ({columns_str})
                        VALUES ({placeholders}) RETURNING {id_column_name};
                    ''', tuple(values))
                    
                new_id = cur.fetchone()[0]
                conn.commit()
        finally:
            self.put_connection(conn)
        return new_id

    # met à jour une ligne existante
    def update_item(self, table_name, item_id, columns, values, column_name=None):
        conn = self.get_connection()
        try:
            with conn.cursor() as cur:
                column_name = column_name or f"id_{table_name.lower()}"

                values = [None if pd.isna(value) or value in ['', 'NaT'] else value for value in values]
    
                set_clause = ', '.join([f'{col} = %s' for col in columns])
                

                cur.execute(f'''
                    UPDATE {table_name}
                    SET {set_clause}
                    WHERE {column_name} = %s ;
                ''', tuple(values) + (item_id,))
        
                conn.commit()
        finally:
            self.put_connection(conn)

    def create_update(self, table_name, item_id, columns, values, column_name=None):
        conn = self.get_connection()
        try:
            with conn.cursor() as cur:
                column_name = column_name or f"id_{table_name.lower()}"

                cur.execute(f'SELECT 1 FROM {table_name} WHERE {column_name} = %s;', (item_id,))
                exists = cur.fetchone()

                values = [None if value == "" else (int(value) if isinstance(value, np.int64) else value) for value in values]
                
                if exists:
                    set_clause = ', '.join([f'{col} = %s' for col in columns])
                    cur.execute(f'''
                        UPDATE {table_name}
                        SET {set_clause}
                        WHERE {column_name} = %s;
                    ''', tuple(values) + (item_id,))
                else:
                    placeholders = ', '.join(['%s'] * len(values))
                    columns_str = ', '.join(columns)
                    cur.execute(f'''
                        INSERT INTO {table_name} ({columns_str})
                        VALUES ({placeholders}) RETURNING {column_name};
                    ''', tuple(values))
                    item_id = cur.fetchone()[0]

                conn.commit()
        finally:
            self.put_connection(conn)

        return item_id
    
    def create_update_batch(self, table_name, column_name, data, columns):
        conn = self.get_connection()
        try:
            with conn.cursor() as cur:
           
                query = f'''
                    INSERT INTO {table_name} ({column_name}, {', '.join(columns)}) 
                    VALUES %s 
                    ON CONFLICT ({column_name}) DO UPDATE
                    SET {', '.join([f"{col} = EXCLUDED.{col}" for col in columns])};
                '''
                
              
                values = [(row['id_affaires'], *[row[col] for col in columns]) for _, row in data.iterrows()]
                
                execute_values(cur, query, values)
                conn.commit()
        finally:
            self.put_connection(conn)


    #supprimer une ligne existante
    def delete_item(self, table_name, item_id):
        conn = self.get_connection()
        try:
            with conn.cursor() as cur:
                id_column_name = f"id_{table_name.lower()}"
                cur.execute(f'DELETE FROM {table_name} WHERE {id_column_name}= %s;', (item_id,))
                conn.commit()
        finally:
            self.put_connection(conn)

    # recupere requete depuis fichier requetes
    def get_query(self, nom_requete):
        if nom_requete in queries:
            return queries[nom_requete].strip()
        raise ValueError(f'Requête {nom_requete} introuvable')
    
            
    # jointure 
    
    def execute_custom_query(self, query_name, filters=None):
        # recup la requete de base
        base_query = self.get_query(query_name)
        
        # Initialize les parametres
        where_clauses = []
        params = []

        # si filtres, les ajoute à la requête
        if filters:
            for key, value in filters.items():
                # sépare le nom de la table et le nom de la colonne
                table_column = key.split('.')
                if len(table_column) == 2:
                    table, column = table_column
                    where_clauses.append(f"{table}.{column} = %s")
                    params.append(value)

        # ajoute les filtres à la requête
        if where_clauses:
            base_query += ' WHERE ' + ' AND '.join(where_clauses)

        # execute la requête
        conn = self.get_connection()
        
        try:
            with conn.cursor() as cur:
                cur.execute(base_query, tuple(params))
                results = cur.fetchall()
                description = cur.description
        finally:
            self.put_connection(conn)

        return results, description



    def fetch_by_id_join(self, nom_requete, params):
        query = self.get_query(nom_requete)
        conn = self.get_connection()
        try:
            with conn.cursor() as cur:
                cur.execute(query, params)
                result = cur.fetchone()
                description = cur.description
        finally:
            self.put_connection(conn)
        return result, description
    

    def truncate_table(self, table_name):
        conn = self.get_connection()
        try:
            with conn.cursor() as cur:
               
                cur.execute(f'TRUNCATE {table_name} CASCADE;')
                sequence_name = f"{table_name}_id_{table_name}_seq"
                cur.execute(f'ALTER SEQUENCE {sequence_name} RESTART WITH 1;')
                conn.commit()
        finally:
            self.put_connection(conn)

            return f'Table {table_name} reset'
        
    def delete_specifc_row(self, table_name,column_name, item_id):
        conn = self.get_connection()
        try:
            with conn.cursor() as cur:
                cur.execute(f'DELETE FROM {table_name} WHERE {column_name}= %s;', (item_id,))
                conn.commit()
        finally:
            self.put_connection(conn)