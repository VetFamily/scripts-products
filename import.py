# pip3 install pandas
# sudo apt-get build-dep python3-psycopg2
# pip3 install psycopg2-binary
# pip3 install SQLAlchemy
# pip3 install openpyxl
# pip3 install xlrd


import argparse
import glob
import os
import shutil
from datetime import datetime

import numpy as np
import pandas as pd
import psycopg2
from openpyxl import Workbook
from openpyxl import load_workbook
from openpyxl.utils.dataframe import dataframe_to_rows
from sqlalchemy import create_engine
from sqlalchemy.engine.url import URL
from sqlalchemy.sql import text

from config import config
from src.common import common


def getArguments():
    parser = argparse.ArgumentParser(description='Lancement du traitement du fichier de valorisations Vetapro')
    # parser.add_argument('-o', '--output', help='Output file name', default='stdout')
    required_args = parser.add_argument_group('required named arguments')
    required_args.add_argument('-c', '--country', help='ID of country', required=True)
    return parser.parse_args()


def insert_new_product(df):
    """Create new product in database

    Parameters:
    row (DataFrame row) : row of products Dataframe

    Returns:
    int : ID of created product"""

    global count_of_new_products

    df_temp = df[df['Id'].isnull()].copy()

    df_temp.drop(df_temp.columns.difference(
        ['Code GTIN', 'Autre code GTIN', 'ID classe thérapeutique', 'Dénomination', 'Conditionnement',
         'Laboratoire', 'Obsolète', 'Invisible']), axis=1, inplace=True)
    df_temp = df_temp.drop_duplicates()
    df_temp.columns = ['code_gtin', 'code_gtin_autre', 'famille_therapeutique_id', 'denomination',
                       'conditionnement', 'laboratoire_id', 'obsolete', 'invisible']
    # df_temp.loc[df_temp['Code GTIN'].notnull() & (str(df_temp['Code GTIN']) == ''), 'Code GTIN'] = np.nan
    df_temp.to_sql('produits', engine, if_exists='append', index=False, chunksize=1000)
    count_of_new_products = len(df_temp.index)

    del df_temp


def insert_types(df):
    global df_logs, count_of_types

    df_temp = df[df['Id'].isnull()].copy()
    df_temp.drop(df_temp.columns.difference(['produit_id', 'Types']), axis=1, inplace=True)

    # Log products without types
    df_without_types = df_temp.loc[df_temp['Types'].isnull(), :]
    for index, row in df_without_types.iterrows():
        df_logs = df_logs.append(
            pd.DataFrame([[os.path.basename(f), row['produit_id'], 'Erreur', 'Pas de type renseigné']
                          ]),
            ignore_index=True)

    df_temp = df_temp.loc[df_temp['Types'].notnull(), :]

    if len(df_temp.index) > 0:
        df_types = pd.DataFrame(df_temp['Types'].str.split('|').tolist(), index=df_temp['produit_id']).stack()
        df_types = df_types.reset_index()[[0, 'produit_id']]
        df_types.columns = ['type_id', 'produit_id']
        df_types = df_types.replace(
            {'type_id': {"Aliment": 1, "Antibiotique": 2, "Divers": 3, "Matériel": 4, "Médicament": 5, "Biocide": 6}})
        df_types = df_types.drop_duplicates()
        df_types.to_sql('produit_type', engine, if_exists='append', index=False, chunksize=1000)
        count_of_types = len(df_types.index)

        del df_types

    del df_temp


def update_product(df):
    global count_of_upd_products

    df_temp = df[df['Id'].notnull() & (df['Code GTIN'].notnull() | df['Autre code GTIN'].notnull())].copy()

    for index, row in df_temp.iterrows():
        update = False

        if row['Code GTIN'] is not None and pd.notna(row['Code GTIN']):
            query = text("""UPDATE produits SET code_gtin = :code_gtin WHERE id = :id""")
            res = connection.execute(query, code_gtin=row['Code GTIN'], id=row['Id'])
            if res.rowcount == 1:
                update = True

        if row['Autre code GTIN'] is not None and pd.notna(row['Autre code GTIN']):
            query = text("""UPDATE produits SET code_gtin_autre = :code_gtin WHERE id = :id""")
            res = connection.execute(query, code_gtin=row['Autre code GTIN'], id=row['Id'])
            if res.rowcount == 1:
                update = True

        if update:
            count_of_upd_products += 1

    del df_temp


def insert_species(df):
    global df_logs, count_of_species

    df_temp = df[df['Id'].isnull()].copy()
    df_temp.drop(df_temp.columns.difference(['produit_id', 'Espèces']), axis=1, inplace=True)

    # Log products without species
    df_without_species = df_temp.loc[df_temp['Espèces'].isnull(), :]
    for index, row in df_without_species.iterrows():
        df_logs = df_logs.append(
            pd.DataFrame([[os.path.basename(f), row['produit_id'], 'Erreur', 'Pas d\'espèce renseignée']
                          ]),
            ignore_index=True)

    df_temp = df_temp.loc[df_temp['Espèces'].notnull(), :]

    if len(df_temp.index) > 0:
        df_species = pd.DataFrame(df_temp['Espèces'].str.split('|').tolist(), index=df_temp['produit_id']).stack()
        df_species = df_species.reset_index()[[0, 'produit_id']]
        df_species.columns = ['espece_id', 'produit_id']
        df_species = df_species.replace(
            {'espece_id': {"Canine": 1, "Equine":2, "Rurale": 3, "Porc": 4, "Volaille": 5, "Autres": 6}})
        df_species = df_species.drop_duplicates()
        df_species.to_sql('espece_produit', engine, if_exists='append', index=False, chunksize=1000)
        count_of_species = len(df_species.index)

        del df_species

    del df_temp


def insert_central_codes(df, cent_id, cent_name):
    global df_logs

    count = 0
    date = pd.to_datetime("2016-01-01", format='%Y-%m-%d', errors='coerce')

    df_temp = df[df['Code_' + cent_name].notnull()].copy()
    if cent_id in [18, 19]:
        df_temp.drop(df_temp.columns.difference(
            ['produit_id', 'Code_' + cent_name, 'Dénomination_' + cent_name, 'Tarif_' + cent_name,
             'Condition_commerciale_' + cent_name]), axis=1, inplace=True)
        df_temp.columns = [
            'code_produit', 'nom', 'prix_unitaire_hors_promo', 'cirrina_pricing_condition_id', 'produit_id']
    else:
        df_temp.drop(df_temp.columns.difference(
            ['produit_id', 'Code_' + cent_name, 'Dénomination_' + cent_name, 'Tarif_' + cent_name]), axis=1,
                     inplace=True)
        df_temp['cirrina_pricing_condition_id'] = np.nan
        df_temp.columns = ['code_produit', 'nom', 'prix_unitaire_hors_promo', 'produit_id',
                           'cirrina_pricing_condition_id']
    df_temp['code_produit'] = df_temp['code_produit'].dropna().apply(lambda x: str(x))
    df_temp['nom'] = df_temp['nom'].dropna().apply(lambda x: str(x))
    df_temp['cirrina_pricing_condition_id'] = pd.to_numeric(df_temp['cirrina_pricing_condition_id'])

    query_cp = text("""
                        SELECT id as centrale_produit_id, code_produit, cirrina_pricing_condition_id
                        FROM centrale_produit
                        WHERE centrale_id = :sourceId
                        AND country_id = :countryId
                                                        """)
    df_cp = pd.read_sql_query(query_cp, connection, params={'sourceId': cent_id, 'countryId': country_id})
    df_cp['cirrina_pricing_condition_id'] = pd.to_numeric(df_cp['cirrina_pricing_condition_id'])
    df_temp = pd.merge(df_temp, df_cp, on=['code_produit', 'cirrina_pricing_condition_id'], how='left')
    del df_cp

    if country_id != 1:
        insert_product_country_label(df_temp, False)

    df_temp_upd = df_temp.loc[df_temp['centrale_produit_id'].notnull(), :].copy()

    for index, row in df_temp_upd.iterrows():
        # Update code because it already exists
        query_upd = text("""UPDATE centrale_produit SET produit_id = :prodId WHERE id = :id""")
        res_upd = connection.execute(query_upd, id=row['centrale_produit_id'], prodId=row['produit_id'])
        if res_upd.rowcount != 1:
            df_logs = df_logs.append(pd.DataFrame(
                [[os.path.basename(f), row['produit_id'], 'Erreur', 'Code ' + cent_name + ' non mis à jour']]),
                                     ignore_index=True)
        else:
            count += 1

    df_temp_new = df_temp.loc[df_temp['centrale_produit_id'].isnull(), :].copy()

    # Insert into centrale_produit
    df_temp_new_cp = df_temp_new[['produit_id', 'code_produit', 'cirrina_pricing_condition_id']]
    df_temp_new_cp.insert(0, 'centrale_id', cent_id)
    df_temp_new_cp.insert(0, 'country_id', country_id)
    df_temp_new_cp = df_temp_new_cp.drop_duplicates()
    df_temp_new_cp.to_sql('centrale_produit', engine, if_exists='append', index=False, chunksize=1000)
    count += len(df_temp_new_cp.index)

    df_temp_new = df_temp_new.drop('centrale_produit_id', axis=1)
    df_temp_new = pd.merge(df_temp_new, pd.read_sql_query(query_cp, connection,
                                                          params={'sourceId': cent_id, 'countryId': country_id}),
                           on=['code_produit', 'cirrina_pricing_condition_id'], how='left')

    if len(df_temp_new.index) > 0:
        # Insert into centrale_produit_denominations
        df_temp_new_cpd = df_temp_new[['centrale_produit_id', 'nom']]
        df_temp_new_cpd.insert(0, 'date_creation', date)
        df_temp_new_cpd = df_temp_new_cpd.drop_duplicates()
        df_temp_new_cpd.to_sql('centrale_produit_denominations', engine, if_exists='append', index=False,
                               chunksize=1000)

        del df_temp_new_cpd

    count_of_centrals_codes[cent_name] = count

    del df_temp, df_temp_upd, df_temp_new


def insert_product_country(df):
    query = text("""
                    select distinct product_id from product_country
                    where country_id = :countryId
                    """)
    df_tmp = pd.read_sql_query(query, connection, params={'countryId': country_id})
    # Formatting columns
    df_tmp['product_id'] = pd.to_numeric(df_tmp['product_id'])

    df_products = df.loc[(~df['produit_id'].isin(df_tmp['product_id'])), :].copy()
    if len(df_products.index) > 0:
        df_products = df_products[['produit_id']]
        df_products.rename(columns={'produit_id': 'product_id'}, inplace=True)
        df_products.insert(0, 'country_id', country_id)
        df_products = df_products.drop_duplicates()
        df_products.to_sql('product_country', engine, if_exists='append', index=False, chunksize=1000)

    del df_products, df_tmp


def insert_product_country_label(df, is_france):
    query = text("""
                        select distinct l.code as label_code, language_id
                        from label l
                        join country c on c.default_language_id = l.language_id
                        where l.code like 'PROD%'
                """)
    df_tmp = pd.read_sql_query(query, connection, params={'countryId': country_id})

    df_products_labels = df.copy()
    if len(df_products_labels.index) > 0:
        query_language = text("""select default_language_id from country where id = :countryId""")
        language_id = connection.execute(query_language, countryId=country_id).first()[0]

        if is_france:
            product_name = 'Dénomination'
            product_packaging = 'Conditionnement'
            df_products_labels.loc[df_products_labels[product_packaging].isnull(), product_packaging] = ''
        else:
            product_name = 'nom'
            product_packaging = 'product_packaging'
            df_products_labels[product_packaging] = ''

        df_products_labels['name_label'] = df_products_labels.agg(lambda x: f"PROD{str(int(x['produit_id']))}N", axis=1)
        df_products_labels['packaging_label'] = df_products_labels.agg(lambda x: f"PROD{str(int(x['produit_id']))}P",
                                                                       axis=1)
        df_products_labels.insert(0, 'language_id', language_id)

        # Label of name of product
        df_products_labels_name = df_products_labels.loc[(~df_products_labels['name_label'].isin(df_tmp['label_code']) &
                                                          df_products_labels[product_name].notnull()), :]

        if len(df_products_labels_name.index) > 0:
            df_products_labels_name = df_products_labels_name[['name_label', product_name, 'language_id']]
            df_products_labels_name.drop_duplicates(subset='name_label', keep='first', inplace=True)
            df_products_labels_name.rename(columns={'name_label': 'code', product_name: 'value'}, inplace=True)
            df_products_labels_name = df_products_labels_name.drop_duplicates()
            df_products_labels_name.to_sql('label', engine, if_exists='append', index=False, chunksize=1000)

        # Label of packaging of product
        df_products_labels_pack = df_products_labels.loc[(~df_products_labels['packaging_label'].isin(
            df_tmp['label_code']) & df_products_labels[product_packaging].notnull()), :]

        if len(df_products_labels_pack.index) > 0:
            df_products_labels_pack = df_products_labels_pack[['packaging_label', product_packaging, 'language_id']]
            df_products_labels_pack.drop_duplicates(subset='packaging_label', keep='first', inplace=True)
            df_products_labels_pack.rename(columns={'packaging_label': 'code', product_packaging: 'value'}, inplace=True)
            df_products_labels_pack = df_products_labels_pack.drop_duplicates()
            df_products_labels_pack.to_sql('label', engine, if_exists='append', index=False, chunksize=1000)

    del df_products_labels, df_tmp


def process():
    df = df_init.where(pd.notnull(df_init), None)
    df['Obsolète'] = df['Obsolète'].apply(lambda x: True if x == 'True' else False)
    df['Invisible'] = df['Invisible'].apply(lambda x: True if x == 'True' else False)
    df['Id'] = pd.to_numeric(df['Id'])
    df['Laboratoire'] = pd.to_numeric(df['Laboratoire'])
    df['Code GTIN'] = df['Code GTIN'].dropna().apply(lambda x: str(int(x)))
    df['Autre code GTIN'] = df['Autre code GTIN'].dropna().apply(lambda x: str(int(x)) if type(x) is float else str(x))
    df['Dénomination'] = df['Dénomination'].dropna().apply(lambda x: str(x))
    df['Conditionnement'] = df['Conditionnement'].dropna().apply(lambda x: str(x))

    # Insert new products
    insert_new_product(df)

    # Add products IDs
    query_products = text("""
                SELECT id as produit_id, denomination, conditionnement, laboratoire_id, obsolete, invisible, code_gtin 
                FROM produits""")
    df_products = pd.read_sql_query(query_products, connection)
    df_products['produit_id'] = pd.to_numeric(df_products['produit_id'])
    df_products['laboratoire_id'] = pd.to_numeric(df_products['laboratoire_id'])

    temp = pd.merge(df[pd.isnull(df['Id'])], df_products,
                    left_on=['Code GTIN', 'Dénomination', 'Conditionnement', 'Laboratoire', 'Obsolète', 'Invisible'],
                    right_on=['code_gtin', 'denomination', 'conditionnement', 'laboratoire_id', 'obsolete',
                              'invisible'], how='left')
    df = pd.concat([temp, df[pd.notnull(df['Id'])]], axis=0, sort=False, ignore_index=True)
    df.loc[df['produit_id'].isnull(), 'produit_id'] = df['Id']
    df['produit_id'] = pd.to_numeric(df['produit_id'])

    # Add Vetapro codes
    if country_id == 1:
        df.loc[(df['Id'].isnull() & df['Code_Vetapro'].isnull()), 'Code_Vetapro'] = df['produit_id']

    # Add country ID for products
    insert_product_country(df)

    if country_id == 1:
        insert_product_country_label(df, True)

    # Remove .0 from codes columns (not for Cirrina and Serviphar)
    for col in df.columns:
        if 'Code_' in col:
            if col in ['Code_Cirrina', 'Code_Serviphar']:
                df[col] = df[col].dropna().apply(lambda x: str(x))
            else:
                df[col] = df[col].dropna().apply(lambda x: str(int(x)) if type(x) is float else x)

    # Insert types of new products
    insert_types(df)

    # Insert species of new products
    insert_species(df)

    # Update products
    update_product(df)

    # Insert Alcyon codes
    insert_central_codes(df, 1, 'Alcyon')

    # Insert Centravet codes
    insert_central_codes(df, 2, 'Centravet')

    # Insert Coveto codes
    insert_central_codes(df, 3, 'Coveto')

    # Insert Alibon codes
    insert_central_codes(df, 4, 'Alibon')

    # Insert Vetapro codes
    insert_central_codes(df, 5, 'Vetapro')

    # Insert Vetys codes
    insert_central_codes(df, 6, 'Vetys')

    # Insert Hippocampe codes
    insert_central_codes(df, 7, 'Hippocampe')

    # Insert Agripharm codes
    insert_central_codes(df, 8, 'Agripharm')

    # Insert Elvetis codes
    insert_central_codes(df, 9, 'Elvetis')

    # Insert Longimpex codes
    insert_central_codes(df, 10, 'Longimpex')

    # Insert Direct Biové codes
    insert_central_codes(df, 11, 'Direct')

    # Insert Cedivet codes
    insert_central_codes(df, 12, 'Cedivet')

    # Insert Covetrus codes
    insert_central_codes(df, 13, 'Covetrus')

    # Insert Apoex codes
    insert_central_codes(df, 15, 'Apoex')

    # Insert Kruuse codes
    insert_central_codes(df, 16, 'Kruuse')

    # Insert Apotek1 codes
    insert_central_codes(df, 17, 'Apotek1')

    # Insert Cirrina codes
    insert_central_codes(df, 18, 'Cirrina')

    # Insert Serviphar codes
    insert_central_codes(df, 19, 'Serviphar')

    # Insert Soleomed codes
    insert_central_codes(df, 20, 'Soleomed')


def create_excel_file(filename, df, append):
    if append:
        wb = load_workbook(filename)
    else:
        wb = Workbook()
    ws = wb.active

    for r in dataframe_to_rows(df, index=False, header=(False if append else True)):
        ws.append(r)
    wb.save(filename)


if __name__ == "__main__":
    try:
        # Reading parameters of database
        params = config(section="postgresql")

        # Connecting to the PostgreSQL server
        print('Connecting to the PostgreSQL database...')
        engine = create_engine(URL(**params), echo=False)
        connection = engine.connect()

        print(f'** Processing new products **')

        # Getting args
        args = getArguments()
        country_id = int(args.country)
        country_name = common.get_name_of_country(connection, country_id)

        now = datetime.now().strftime('%Y%m%d')

        initDir = './fichiers/nouveaux/' + country_name + "/"
        workDir = './encours/nouveaux/' + country_name + "/"
        historicDir = './historiques/nouveaux/' + country_name + "/" + now + "/"
        logDir = './logs/nouveaux/' + country_name + "/" + now + "/"

        # Create directories if not exist
        os.makedirs(workDir, exist_ok=True)
        os.makedirs(historicDir, exist_ok=True)
        os.makedirs(logDir, exist_ok=True)

        # Create empty logs dataframes
        df_logs = pd.DataFrame(columns=['Fichier', 'ID Produit', 'Type', 'Description'])
        if not os.path.isfile(logDir + "products_errors.xlsx"):
            create_excel_file(logDir + "products_errors.xlsx", df_logs, False)

        for f in glob.glob(r'' + initDir + '/*.[xX][lL][sS][xX]'):
            print(f'Processing file "{os.path.basename(f)}" ...')

            # Begin transaction
            trans = connection.begin()

            # Move file to working directory
            shutil.move(f, workDir + os.path.basename(f))

            # Initialize counts
            count_of_new_products = 0
            count_of_types = 0
            count_of_species = 0
            count_of_upd_products = 0
            count_of_centrals_codes = {"Alcyon": 0, "Centravet": 0, "Coveto": 0, "Alibon": 0, "Vetapro": 0,
                                       "Vetys": 0, "Hippocampe": 0, "Agripharm": 0, "Elvetis": 0, "Longimpex": 0,
                                       "Direct": 0, "Cedivet": 0, "Covetrus": 0, "Apoex": 0, "Kruuse": 0,
                                       "Apotek1": 0, "Cirrina": 0, "Serviphar": 0, "Soleomed": 0}
            if os.stat(workDir + os.path.basename(f)).st_size > 0:
                # Read Excel file
                df_init = pd.read_excel(workDir + os.path.basename(f), dtype=str)
                # Process file
                process()
            else:
                print(f'File "{os.path.basename(f)}" is empty !')

            # Move source file
            shutil.move(workDir + os.path.basename(f), historicDir + os.path.basename(f))

            trans.commit()
            print(f'File "{os.path.basename(f)}" is treated :')
            print(f'\t{count_of_new_products} products created')
            print(f'\t{count_of_types} types created')
            print(f'\t{count_of_species} species created')
            print(f'\t{count_of_upd_products} products updated')
            for cent in count_of_centrals_codes:
                print(f'\t{count_of_centrals_codes[cent]} codes created or updated for {cent}')

        # Create Excel file of products errors
        create_excel_file(logDir + "products_errors.xlsx", df_logs.drop_duplicates(), True)

    except (Exception, psycopg2.Error) as error:
        if connection:
            print("Error : ", error)

    finally:
        # closing database connection.
        if connection:
            connection.close()
            print("PostgreSQL connection is closed")
