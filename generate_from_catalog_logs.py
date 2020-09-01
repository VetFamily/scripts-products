# pip3 install pandas
# pip3 install argparse
# pip3 install xlrd


import argparse
import glob
import logging
import os
import re
import shutil
from datetime import datetime
from pathlib import Path

import numpy as np
import pandas as pd
import psycopg2
from sqlalchemy import create_engine
from sqlalchemy.engine.url import URL
from sqlalchemy.sql import text

from config import config
from src.common import constant, common


def get_arguments():
    parser = argparse.ArgumentParser(description='Generation du fichier des nouveaux produits')
    # parser.add_argument('-o', '--output', help='Output file name', default='stdout')
    required_args = parser.add_argument_group('required named arguments')
    required_args.add_argument('-c', '--country', help='ID of country', required=True)
    optional_args = parser.add_argument_group('optional named arguments')
    optional_args.add_argument('-d', '--debug', help='Logging debug in console', action='store_true')
    return parser.parse_args()


def process_products():
    logging.info(f"** Generate new products of catalogs for country '{country_name}' start **")

    products_dir = root_dir + constant.DIR_PRODUCTS + "/" + country_name + "/"
    products_new_dir = products_dir + constant.DIR_NEW + '/'
    products_out_dir = products_dir + constant.DIR_ARCHIVES + '/' + now + "_catalogs/"
    products_out_files_dir = products_out_dir + constant.DIR_FILES + "/"

    file_exist = False
    for filename in os.listdir(products_new_dir):
        if re.search('new_products_*.*', filename):
            file_exist = True
            break

    if file_exist:
        # Create directories if not exist
        os.makedirs(products_out_files_dir, exist_ok=True)

        for f in sorted(glob.glob(r'' + products_new_dir + 'new_products_*.*')):
            filename = os.path.basename(f)
            logging.debug(f'** Move "{filename}" to backup files directory **')
            shutil.move(f, products_out_files_dir + filename)

        df_final = pd.DataFrame(columns=['Id', 'Dénomination_temp', 'Conditionnement_temp', 'Laboratoire_temp',
                                         'Obsolète_temp', 'Invisible_temp', 'ID classe thérapeutique_temp', 'Code GTIN',
                                         'Autre code GTIN', 'ID classe thérapeutique', 'Dénomination',
                                         'Conditionnement', 'Laboratoire', 'Obsolète', 'Invisible', 'Types', 'Espèces',
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
                                         'Code_Direct', 'Dénomination_Direct', 'Tarif_Direct',
                                         'Code_Cedivet', 'Dénomination_Cedivet', 'Tarif_Cedivet',
                                         'Code_Covetrus', 'Dénomination_Covetrus', 'Tarif_Covetrus'])

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

        for f in sorted(glob.glob(r'' + products_out_files_dir + '*.*')):
            logging.debug(f'Processing file "{os.path.basename(f)}" ...')

            df_file = pd.read_excel(f, header=None)

            if len(df_file.index) > 0:
                df_file = df_file.iloc[:, 0:25]
                cent_name = Path(f).stem.split('_')[3]
                if cent_name == 'Alcyon':
                    cent_id = 1
                elif cent_name == 'Centravet':
                    cent_id = 2
                elif cent_name == 'Coveto':
                    cent_id = 3
                elif cent_name == 'Alibon':
                    cent_id = 4
                elif cent_name == 'Vetapro':
                    cent_id = 5
                elif cent_name == 'Vetys':
                    cent_id = 6
                elif cent_name == 'Hippocampe':
                    cent_id = 7
                elif cent_name == 'Agripharm':
                    cent_id = 8
                    for x in range(len(df_file.columns), 25):
                        df_file.insert(x, 'empty', np.nan)
                elif cent_name == 'Elvetis':
                    cent_id = 9
                elif cent_name == 'Longimpex':
                    cent_id = 10
                elif cent_name == 'Direct':
                    cent_id = 11
                elif cent_name == 'Cedivet':
                    cent_id = 12
                elif cent_name == 'Covetrus':
                    cent_id = 13

                df_file.columns = ['code_distributeur', 'code_gtin', 'code_ean', 'code_amm', 'code_remplacement',
                                   'designation', 'categorie', 'famille', 'sous_famille', 'famille_commerciale',
                                   'laboratoire', 'statut', 'classe_therapeutique', 'agrement', 'poids_unitaire',
                                   'sous_unite_revente', 'gestion_stock', 'taux_tva', 'quantite_tarif',
                                   'prix_unitaire_hors_promo', 'prix_unitaire_promo', 'date_debut_promo', 'date_fin_promo',
                                   'quantite_ug', 'code_cip']

                df_file['code_gtin'] = df_file['code_gtin'].dropna().apply(lambda x: str(x).replace(',', '.'))
                df_file['code_gtin'] = pd.to_numeric(df_file['code_gtin'].replace(',', '.'))

                temp = pd.merge(df_file[pd.notnull(df_file['code_gtin'])], df_products, on='code_gtin', how='left')
                df_temp = pd.concat([temp, df_file[pd.isnull(df_file['code_gtin'])]], axis=0, sort=False, ignore_index=True)
                del temp

                df_temp.drop(
                    ['agrement', 'poids_unitaire', 'sous_unite_revente', 'gestion_stock', 'taux_tva', 'quantite_tarif',
                     'prix_unitaire_promo', 'date_debut_promo', 'date_fin_promo', 'quantite_ug', 'code_cip'], axis=1,
                    inplace=True, errors='ignore')
                df_temp['denomination'] = df_temp['designation']
                df_temp['obsolete'] = df_temp['obsolete_temp']
                df_temp['invisible'] = df_temp['invisible_temp']
                df_temp['conditionnement'] = np.nan

                # Laboratories
                # Search existing laboratories in database
                query_labs = text("""
                    select laboratoire_id, nom_laboratoire as laboratoire 
                    from centrale_laboratoire
                    where laboratoire_id is not null and centrale_id = :id""")
                df_labs = pd.read_sql_query(query_labs, connection, params={'id': cent_id})

                df_temp = pd.merge(df_temp, df_labs, on=['laboratoire'], how='left')
                df_temp.loc[df_temp['laboratoire_id'].isnull(), 'laboratoire_id'] = df_temp['laboratoire']

                # Types
                df_temp = df_temp.replace({'categorie': {'ALI': 'Aliment', 'DIV': 'Divers', 'MAT': 'Matériel',
                                                         'MED': 'Médicament', 'ATB': 'Antibiotique'}})
                df_temp.loc[df_temp['sous_famille'].notnull() & (
                        str(df_temp['sous_famille']) == 'ATB'), 'categorie'] = 'Antibiotique'

                # Therapeutic classes
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
                     'invisible', 'categorie', 'sous_famille', 'code_distributeur', 'designation',
                     'prix_unitaire_hors_promo']]
                df_temp.columns = ['Id', 'Dénomination_temp', 'Conditionnement_temp', 'Laboratoire_temp', 'Obsolète_temp',
                                   'Invisible_temp', 'ID classe thérapeutique_temp', 'Code GTIN', 'Autre code GTIN',
                                   'ID classe thérapeutique', 'Dénomination', 'Conditionnement', 'Laboratoire', 'Obsolète',
                                   'Invisible', 'Types', 'Espèces', 'Code_' + cent_name, 'Dénomination_' + cent_name,
                                   'Tarif_' + cent_name]

                df_final = pd.concat([df_final, df_temp.drop_duplicates()], axis=0, sort=False, ignore_index=True)
            else:
                logging.debug("File is empty !")

        # Sort dataframe
        df_final = df_final.sort_values(by=['Laboratoire', 'Code GTIN'])

        logging.debug("Generate files of new products")
        writer = pd.ExcelWriter(
            products_out_dir + now + "_" + country_name + "_new_products_catalogs.xlsx",
            engine='xlsxwriter')
        df_final.to_excel(writer, sheet_name='Sheet 1', index=False)
        workbook = writer.book
        worksheet = writer.sheets['Sheet 1']
        red_format = workbook.add_format()
        red_format.set_bg_color('red')
        for col in ['R', 'U', 'X', 'AA', 'AD', 'AG', 'AJ', 'AM', 'AP', 'AS', 'AV', 'AY', 'BB']:
            worksheet.conditional_format(col + '2:' + col + str(len(df_final.index)),
                                         {'type': 'duplicate', 'format': red_format})
        writer.save()
        shutil.copy(
            products_out_dir + now + "_" + country_name + "_new_products_catalogs.xlsx",
            products_out_dir + now + "_" + country_name + "_new_products_catalogs_source.xlsx")
    else:
        logging.debug(f'...no files to process !')

    logging.info(f"** Generate new products of catalogs for country '{country_name}' end **")


def process_suppliers():
    logging.info(f"** Generate new suppliers of catalogs for country '{country_name}' start **")

    suppliers_dir = root_dir + constant.DIR_SUPPLIERS + "/" + country_name + "/"
    suppliers_new_dir = suppliers_dir + constant.DIR_NEW + '/'
    suppliers_out_dir = suppliers_dir + constant.DIR_ARCHIVES + '/' + now + "_catalogs/"
    suppliers_out_files_dir = suppliers_out_dir + constant.DIR_FILES + "/"

    file_exist = False
    for filename in os.listdir(suppliers_new_dir):
        if re.search('new_suppliers_*.*', filename):
            file_exist = True
            break

    if file_exist:
        # Create directories if not exist
        os.makedirs(suppliers_out_files_dir, exist_ok=True)

        for f in sorted(glob.glob(r'' + suppliers_new_dir + '*.*')):
            filename = os.path.basename(f)
            logging.debug(f'** Move "{filename}" to backup files directory **')
            shutil.move(f, suppliers_out_files_dir + filename)

        # Initialize dataframe of products
        df = pd.DataFrame([])

        logging.debug(f'Aggregate files...')
        for f in sorted(glob.glob(r'' + suppliers_out_files_dir + '*.*')):
            df = pd.concat([df, pd.read_excel(f)], axis=0, sort=False, ignore_index=True)

        df.drop_duplicates(inplace=True)

        # Save dataframe for logs
        writer = pd.ExcelWriter(
            suppliers_out_dir + now + "_unknown_suppliers_aggregated.xlsx",
            engine='xlsxwriter')
        df.to_excel(writer, index=False)
        writer.save()
        logging.debug(f'Aggregated file is generated !')
    else:
        logging.debug(f'...no files to process !')

    logging.info(f"** Generate new suppliers of catalogs for country '{country_name}' end **")


if __name__ == "__main__":
    try:
        # Reading parameters of directory
        params_dir = config(section="directories")
        root_dir = params_dir["root_dir"] + "/"

        # Reading parameters of database
        params_db = config()

        # Connecting to the PostgreSQL server
        logging.debug('Connecting to the PostgreSQL database...')
        engine = create_engine(URL(**params_db), echo=False)
        connection = engine.connect()

        # Getting args
        args = get_arguments()
        country_id = int(args.country)
        country_name = common.get_name_of_country(connection, country_id)
        debug = args.debug

        now = datetime.now().strftime('%Y%m%d%H%M%S')

        # Initialize logging
        if debug:
            logging.basicConfig(format="%(asctime)s  %(levelname)s: %(message)s", level="DEBUG")
        else:
            params_logging = config(section="logging")
            logging.basicConfig(
                filename=params_logging["url"] + "/" + constant.LOG_CATALOGS_FILENAME + constant.LOG_EXTENSION,
                format="%(asctime)s  %(levelname)s: %(message)s", level=params_logging["level"])

        logging.info(f"Generating from catalogs errors start !")

        # Products
        process_products()

        # Suppliers
        process_suppliers()

    except (Exception, psycopg2.Error) as error:
        logging.error(error)

    finally:
        # closing database connection.
        if connection:
            connection.close()
            logging.debug("PostgreSQL connection is closed")
            logging.info(f"Generating from purchases errors end !")
