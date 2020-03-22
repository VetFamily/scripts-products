# pip3 install pandas
# pip3 install argparse
# pip3 install xlrd


import argparse
import numpy as np
from openpyxl import load_workbook
from openpyxl import Workbook
from openpyxl.utils.dataframe import dataframe_to_rows
import os
import pandas as pd
from pathlib import Path
import psycopg2
import glob
from sqlalchemy import create_engine
from sqlalchemy.engine.url import URL
from sqlalchemy.sql import text

from config import config


def getArguments():
    parser = argparse.ArgumentParser(description='Generation du fichier des nouveaux produits')
    # parser.add_argument('-o', '--output', help='Output file name', default='stdout')
    required_args = parser.add_argument_group('required named arguments')
    required_args.add_argument('-d', '--date', help='Dossier contenant les fichiers de logs', required=True)
    return parser.parse_args()


def aggregate_files():
    print('Aggregating files...')

    clients = ['bourgelat', 'vetoavenir', 'vetapro', 'vetapharma', 'vetharmonie', 'cristal', 'symbioveto', 'clubvet',
               'vetodistribution']

    # Aggregate laboratories files
    df_labs = pd.DataFrame([])
    for client in clients:
        dir_client = '/home/ftpusers/amadeo/script-achats/' + client + '/' + date + '/'

        for f in sorted(glob.glob(r'' + dir_client + 'unknown_laboratories*.xlsx')):
            df_labs = pd.concat([df_labs, pd.read_excel(f, header=None)], axis=0, sort=False, ignore_index=True)

    if os.path.isfile(logDir + 'unknown_laboratories.xlsx'):
        os.remove(logDir + 'unknown_laboratories.xlsx')
    create_excel_file(logDir + 'unknown_laboratories.xlsx', df_labs.drop_duplicates(), False)

    # Aggregate products files
    for cent in cents:
        df_cent = pd.DataFrame([])
        for client in clients:
            dir_client = '/home/ftpusers/amadeo/script-achats/' + client + '/' + date + '/'

            for f in sorted(glob.glob(r'' + dir_client + 'unknown_products_' + cent + '*.xlsx')):
                df_cent = pd.concat([df_cent, pd.read_excel(f, header=None)], axis=0, sort=False, ignore_index=True)

        if os.path.isfile(logDir + 'unknown_products_' + cent + '.xlsx'):
            os.remove(logDir + 'unknown_products_' + cent + '.xlsx')
        create_excel_file(logDir + 'unknown_products_' + cent + '.xlsx', df_cent, False)


def process_file(df_file, cent_name, df_products, df_classes, df):
    if cent_name == 'Alcyon':
        cent_id = 1
        nb_of_cols = 14
        columns = ['centrale', 'client_identifiant', 'client_nom', 'laboratoire', 'code_distributeur',
                   'designation', 'taux_tva', 'code_cip', 'code_gtin', 'date_achat', 'qte_payante',
                   'qte_ug', 'ca_complet', 'classe_therapeutique']
    elif cent_name == 'Centravet':
        cent_id = 2
        nb_of_cols = 20
        columns = ['centrale', 'client_livre_nom', 'client_nom', 'code_distributeur', 'code_cip', 'code_gtin',
                   'designation', 'laboratoire', 'classe_1', 'classe_2', 'classe_3', 'mois_achat', 'annee_achat',
                   'ca_complet', 'qte_payante', 'qte_ug', 'client_identifiant', 'categorie_1', 'categorie_2',
                   'categorie_3']
    elif cent_name == 'Coveto':
        cent_id = 3
        nb_of_cols = 14
        columns = ['centrale', 'client_identifiant', 'client_nom', 'laboratoire', 'code_distributeur',
                   'designation', 'taux_tva', 'code_cip', 'code_gtin', 'date_achat', 'qte_payante',
                   'qte_ug_vide', 'ca_complet', 'qte_ug']
    elif cent_name == 'Alibon':
        cent_id = 4
        nb_of_cols = 11
        columns = ['centrale', 'client_identifiant', 'client_nom', 'code_distributeur', 'code_gtin', 'laboratoire',
                   'annee_achat', 'mois_achat', 'designation', 'qte_payante', 'ca_complet']
    elif cent_name == 'Vetapro':
        cent_id = 5
    elif cent_name == 'Vetys':
        cent_id = 6
        nb_of_cols = 11
        columns = ['centrale', 'client_identifiant', 'code_distributeur', 'annee_achat', 'mois_achat', 'code_gtin',
                   'designation', 'qte_payante', 'laboratoire', 'prix_unitaire', 'ca_complet']
    elif cent_name == 'Hippocampe':
        cent_id = 7
        nb_of_cols = 15
        columns = ['centrale', 'client_identifiant', 'client_nom', 'code_distributeur', 'code_cip', 'code_gtin',
                   'designation', 'laboratoire', 'classe_1', 'classe_2', 'classe_3', 'mois_achat',
                   'annee_achat', 'ca_complet', 'qte_payante']
    elif cent_name == 'Agripharm':
        cent_id = 8
        nb_of_cols = 17
        columns = ['centrale', 'client_identifiant', 'client_nom', 'code_distributeur', 'designation',
                   'laboratoire', 'code_gtin', 'code_cip', 'annee_achat', 'mois_achat', 'date_achat', 'qte_payante',
                   'qte_ug', 'ca_complet', 'num_facture', 'client_livre_identifiant', 'client_livre_nom']
    elif cent_name == 'Elvetis':
        cent_id = 9
        nb_of_cols = 11
        columns = ['centrale', 'client_identifiant', 'client_nom', 'laboratoire', 'code_distributeur',
                   'designation', 'code_gtin', 'annee_achat', 'mois_achat', 'qte_payante', 'ca_complet']
    elif cent_name == 'Longimpex':
        cent_id = 10
        nb_of_cols = 11
        columns = ['centrale', 'client_identifiant', 'client_nom', 'laboratoire', 'code_distributeur',
                   'designation', 'code_gtin', 'annee_achat', 'mois_achat', 'qte_payante', 'ca_complet']
    elif cent_name == 'Direct_Biové':
        cent_id = 11
        nb_of_cols = 36
        columns = ['centrale', 'date_achat', 'mois_achat', 'annee_achat', 'client_identifiant', 'client_nom',
                   'a_supprimer', 'code_cip', 'designation', 'qte_payante', 'ca_complet', 'a_supprimer', 'code_gtin',
                   'a_supprimer', 'num_facture', 'a_supprimer', 'client_ville', 'a_supprimer', 'a_supprimer',
                   'a_supprimer', 'client_adresse', 'client_adresse', 'a_supprimer', 'a_supprimer', 'a_supprimer',
                   'a_supprimer', 'a_supprimer', 'a_supprimer', 'laboratoire', 'a_supprimer', 'a_supprimer',
                   'a_supprimer', 'classe_1', 'classe_2', 'a_supprimer', 'code_distributeur']
    elif cent_name == 'Cedivet':
        cent_id = 12
        nb_of_cols = 13
        columns = ['centrale', 'annee_achat', 'mois_achat', 'laboratoire', 'code_gtin', 'client_identifiant',
                   'client_nom', 'client_adresse', 'client_ville', 'code_distributeur', 'designation', 'qte_payante',
                   'ca_complet']

    df_file = df_file.iloc[:, 0:nb_of_cols]
    df_file.columns = columns

    if cent_id != 6:
        if cent_id in [2, 7]:
            df_file['classe_therapeutique'] = df_file['classe_1'] + ' / ' + df_file['classe_2'] + ' / ' + df_file[
                'classe_3']

        if cent_id == 2:
            df_file['qte_payante'] = df_file['qte_payante'] = df_file['qte_ug']
        df_file['prix_unitaire'] = df_file['ca_complet'] / df_file['qte_payante']

    df_file.drop(
        ['centrale', 'client_identifiant', 'client_nom', 'taux_tva', 'code_cip', 'annee_achat', 'mois_achat',
         'date_achat', 'qte_payante', 'qte_ug', 'qte_ug_vide', 'ca_complet', 'num_facture',
         'client_livre_identifiant', 'client_livre_nom', 'classe_1', 'classe_2', 'classe_3', 'categorie_1',
         'categorie_2', 'categorie_3', 'client_adresse', 'client_ville', 'a_supprimer'], axis=1, inplace=True,
        errors='ignore')
    df_file = df_file.drop_duplicates(subset=['code_distributeur', 'designation', 'laboratoire', 'code_gtin'])
    df_file['code_gtin'] = df_file['code_gtin'].dropna().apply(lambda x: str(x).replace(',', '.'))
    df_file['code_gtin'] = pd.to_numeric(df_file['code_gtin'].replace(',', '.'))

    temp = pd.merge(df_file[pd.notnull(df_file['code_gtin'])], df_products, on='code_gtin', how='left')
    df_temp = pd.concat([temp, df_file[pd.isnull(df_file['code_gtin'])]], axis=0, sort=False, ignore_index=True)
    del temp

    df_temp['denomination'] = df_temp['designation']
    df_temp['obsolete'] = df_temp['obsolete_temp']
    df_temp['invisible'] = df_temp['invisible_temp']
    df_temp['conditionnement'] = np.nan
    df_temp['types'] = np.nan
    df_temp['especes'] = np.nan

    # Laboratories
    # Search existing laboratories in database
    query_labs = text("""
        select laboratoire_id, nom_laboratoire as laboratoire 
        from centrale_laboratoire
        where laboratoire_id is not null and centrale_id = :id""")
    df_labs = pd.read_sql_query(query_labs, connection, params={'id': cent_id})

    df_temp = pd.merge(df_temp, df_labs, on=['laboratoire'], how='left')
    df_temp.loc[df_temp['laboratoire_id'].isnull(), 'laboratoire_id'] = df_temp['laboratoire']

    # Therapeutic classes
    if 'classe_therapeutique' not in df_temp.columns:
        df_temp['classe_therapeutique'] = np.nan
    df_temp['famille_therapeutique'] = df_temp['classe_therapeutique'].dropna().apply(
        lambda x: str(x).replace(' ', '')[:4])
    temp = pd.merge(df_temp[pd.notnull(df_temp['famille_therapeutique'])], df_classes,
                    on=['famille_therapeutique'], how='left')
    df_temp = pd.concat([temp, df_temp[pd.isnull(df_temp['famille_therapeutique'])]], axis=0, sort=False,
                        ignore_index=True)
    del temp

    df_temp = df_temp[
        ['produit_id', 'denomination_temp', 'conditionnement_temp', 'laboratoire_id_temp', 'obsolete_temp',
         'invisible_temp', 'famille_therapeutique_id_temp', 'code_gtin', 'classe_therapeutique',
         'famille_therapeutique_id', 'denomination', 'conditionnement', 'laboratoire_id', 'obsolete',
         'invisible', 'types', 'especes', 'code_distributeur', 'designation', 'prix_unitaire']]
    df_temp.columns = ['Id', 'Dénomination_temp', 'Conditionnement_temp', 'Laboratoire_temp', 'Obsolète_temp',
                       'Invisible_temp', 'ID classe thérapeutique_temp', 'Code GTIN', 'Autre code GTIN',
                       'ID classe thérapeutique', 'Dénomination', 'Conditionnement', 'Laboratoire', 'Obsolète',
                       'Invisible', 'Types', 'Espèces', 'Code_' + cent_name, 'Dénomination_' + cent_name,
                       'Tarif_' + cent_name]

    df = pd.concat([df, df_temp.drop_duplicates()], axis=0, sort=False, ignore_index=True)

    # Sort dataframe
    df = df.sort_values(by=['Laboratoire', 'Code GTIN'])

    return df


def process():
    print('Processing files...')
    df = pd.DataFrame(columns=['Id', 'Dénomination_temp', 'Conditionnement_temp', 'Laboratoire_temp',
                               'Obsolète_temp', 'Invisible_temp', 'ID classe thérapeutique_temp', 'Code GTIN',
                               'Autre code GTIN', 'ID classe thérapeutique', 'Dénomination', 'Conditionnement',
                               'Laboratoire', 'Obsolète', 'Invisible', 'Types', 'Espèces',
                               'Code_Alcyon', 'Dénomination_Alcyon', 'Tarif_Alcyon',
                               'Code_Centravet', 'Dénomination_Centravet', 'Tarif_Centravet',
                               'Code_Coveto', 'Dénomination_Coveto', 'Tarif_Coveto',
                               'Code_Alibon', 'Dénomination_Alibon', 'Tarif_Alibon',
                               'Code_Vetapro', 'Dénomination_Vetapro', 'Tarif_Vetapro',
                               'Code_Vetys', 'Dénomination_Vetys', 'Tarif_Vetys',
                               'Code_Hippocampe', 'Dénomination_Hippocampe', 'Tarif_Hippocampe',
                               'Code_Agripharm', 'Dénomination_Agripharm', 'Tarif_Agripharm',
                               'Code_Elvetis', 'Dénomination_Elvetis', 'Tarif_Elvetis',
                               'Code_Longimpex', 'Dénomination_Longimpex', 'Tarif_Longimpex',
                               'Code_Direct_Biové', 'Dénomination_Direct_Biové', 'Tarif_Direct_Biové',
                               'Code_Cedivet', 'Dénomination_Cedivet', 'Tarif_Cedivet'])

    if os.path.isfile(logDir + date[0:6] + "_Nouveaux produits_achats.xlsx"):
        os.remove(logDir + date[0:6] + "_Nouveaux produits_achats.xlsx")

    # Search existing products in database
    query_products = text("""
        select id as produit_id, denomination as denomination_temp, conditionnement as conditionnement_temp, 
            laboratoire_id as laboratoire_id_temp, obsolete as obsolete_temp, invisible as invisible_temp, 
            famille_therapeutique_id as famille_therapeutique_id_temp, code_gtin
        from produits
        where code_gtin is not null""")
    df_products = pd.read_sql_query(query_products, connection)
    df_products['code_gtin'] = pd.to_numeric(df_products['code_gtin'])

    # Search existing therapeutic classes in database
    query_classes = text("""
        select id as famille_therapeutique_id, CONCAT(classe1_code, coalesce(classe2_code, ''), 
            coalesce(classe3_code, '')) as famille_therapeutique
        from familles_therapeutiques""")
    df_classes = pd.read_sql_query(query_classes, connection)

    # Aggregate all files of new products
    for f in sorted(glob.glob(r'' + logDir + 'unknown_products_*.xlsx')):
        print(f'Processing file "{os.path.basename(f)}" ...')

        if os.stat(f).st_size > 0:
            df_file = pd.read_excel(f, header=None)
            if len(df_file.index) > 0:
                cent_name = Path(f).stem.split('_')[2]
                df = process_file(df_file, cent_name, df_products, df_classes, df)
                # Create Excel file
                print("Generating Excel file...")
                create_excel_file(logDir + date[0:6] + "_Nouveaux produits_achats.xlsx", df, True)
            else:
                print(f'File "{os.path.basename(f)}" is empty !')
        else:
            print(f'File "{os.path.basename(f)}" is empty !')


def create_excel_file(filename, df, header):
    wb = Workbook()
    ws = wb.active

    for r in dataframe_to_rows(df, index=False, header=header):
        ws.append(r)
    wb.save(filename)


if __name__ == "__main__":
    # Getting args
    args = getArguments()
    date = args.date

    logDir = '/home/ftpusers/amadeo/script-achats/elia-digital/' + date + "/"

    # Create directories if not exist
    os.makedirs(logDir, exist_ok=True)

    cents = ['Alcyon', 'Centravet', 'Coveto', 'Alibon', 'Vetapro', 'Vetys', 'Hippocampe', 'Agripharm', 'Elvetis',
             'Longimpex', 'Direct_5_Biové', 'Cedivet']

    print(f'** Generate file of new products of purchases **')

    try:
        # Reading parameters of database
        params = config()

        # Connecting to the PostgreSQL server
        print('Connecting to the PostgreSQL database...')
        engine = create_engine(URL(**params), echo=False)
        connection = engine.connect()

        aggregate_files()
        process()

    except (Exception, psycopg2.Error) as error:
        print("Failed : ", error)
        # trans.rollback()
        raise

    finally:
        # closing database connection.
        if connection:
            connection.close()
            print("PostgreSQL connection is closed")
