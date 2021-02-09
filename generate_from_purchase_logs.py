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


def insert_source_code(df, source_id):
    df_source_tmp = df.copy()
    date = pd.to_datetime("2016-01-01", format='%Y-%m-%d', errors='coerce')

    df_source_tmp = df_source_tmp.drop(columns=['centrale_produit_id'])
    query_product_sources = text("""
        select id as centrale_produit_id, code_produit as product_code, cirrina_pricing_condition_id
        from centrale_produit
        where centrale_id = :sourceId 
        and country_id = :countryId""")
    df_tmp = pd.read_sql_query(query_product_sources, connection,
                               params={"sourceId": source_id, "countryId": country_id})
    df_tmp['cirrina_pricing_condition_id'] = pd.to_numeric(df['cirrina_pricing_condition_id'])
    df_source_tmp = pd.merge(df_source_tmp, df_tmp, on=['product_code', 'cirrina_pricing_condition_id'], how='left')
    df_source_tmp = df_source_tmp.loc[df_source_tmp['centrale_produit_id'].isnull(), :]
    del df_tmp

    # Insert into centrale_produit
    df_temp_new_cp = df_source_tmp[['product_code', 'cirrina_pricing_condition_id']].copy()
    df_temp_new_cp.columns = ['code_produit', 'cirrina_pricing_condition_id']
    df_temp_new_cp.drop_duplicates(inplace=True)
    df_temp_new_cp.insert(0, 'centrale_id', source_id)
    df_temp_new_cp.insert(0, 'country_id', country_id)
    df_temp_new_cp = df_temp_new_cp.drop_duplicates()
    df_temp_new_cp.to_sql('centrale_produit', engine, if_exists='append', index=False, chunksize=1000)
    logging.debug(f"Table centrale_produit : {len(df_temp_new_cp.index)} elements created")

    # Search existing codes sources of products in database
    df_source_tmp = df_source_tmp.drop(columns=['centrale_produit_id'])
    df_tmp = pd.read_sql_query(query_product_sources, connection,
                               params={"sourceId": source_id, "countryId": country_id})
    df_tmp['cirrina_pricing_condition_id'] = pd.to_numeric(df_tmp['cirrina_pricing_condition_id'])
    df_source_tmp = pd.merge(df_source_tmp, df_tmp, on=['product_code', 'cirrina_pricing_condition_id'], how='left')
    del df_tmp

    if len(df_source_tmp.index) > 0:
        # Insert into centrale_produit_denominations
        df_temp_new_cpd = df_source_tmp[['centrale_produit_id', 'product_name']].copy()
        df_temp_new_cpd.drop_duplicates(inplace=True)
        df_temp_new_cpd.columns = ['centrale_produit_id', 'nom']
        df_temp_new_cpd.insert(0, 'date_creation', date)
        df_temp_new_cpd = df_temp_new_cpd.drop_duplicates()
        df_temp_new_cpd.to_sql('centrale_produit_denominations', engine, if_exists='append', index=False,
                               chunksize=1000)
        logging.debug(f"Table centrale_produit_denominations : {len(df_temp_new_cpd.index)} elements created")

        del df_temp_new_cpd

    del df_source_tmp, df_temp_new_cp


def process_products():
    logging.info(f"** Generate new products of purchases for country '{country_name}' start **")

    products_dir = root_dir + constant.DIR_PRODUCTS + "/" + country_name + "/"
    products_new_dir = products_dir + constant.DIR_NEW + '/'
    products_out_dir = products_dir + constant.DIR_ARCHIVES + '/' + now + "_purchases/"
    products_out_files_dir = products_out_dir + constant.DIR_FILES + "/"

    file_exist = False
    for filename in os.listdir(products_new_dir):
        if re.search('unknown_products_*.*', filename):
            file_exist = True
            break

    if file_exist:
        # Create directories if not exist
        os.makedirs(products_out_files_dir, exist_ok=True)

        for f in sorted(glob.glob(r'' + products_new_dir + 'unknown_products_*.*')):
            filename = os.path.basename(f)
            logging.debug(f'** Move "{filename}" to backup files directory **')
            shutil.move(f, products_out_files_dir + filename)

        # Initialize dataframe of products
        df = pd.DataFrame([])

        logging.debug(f'Aggregate files...')
        for f in sorted(glob.glob(r'' + products_out_files_dir + '*.*')):
            df = pd.concat([df, pd.read_excel(f, dtype=str)], axis=0, sort=False, ignore_index=True)

        # Save dataframe for logs
        writer = pd.ExcelWriter(
            products_out_dir + now + "_unknown_products_aggregated.xlsx",
            engine='xlsxwriter')
        df.to_excel(writer, index=False)
        writer.save()
        logging.debug(f'Aggregated file is generated !')

        # Drop duplicates
        df.drop_duplicates(inplace=True)

        # Ignore rows without product code
        df = df[df['product_code'].notnull()]

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
                                         'Code_Covetrus', 'Dénomination_Covetrus', 'Tarif_Covetrus',
                                         'Code_Apoex', 'Dénomination_Apoex', 'Tarif_Apoex',
                                         'Code_Kruuse', 'Dénomination_Kruuse', 'Tarif_Kruuse',
                                         'Code_Apotek1', 'Dénomination_Apotek1', 'Tarif_Apotek1',
                                         'Code_Cirrina', 'Dénomination_Cirrina', 'Tarif_Cirrina',
                                         'Condition_commerciale_Cirrina',
                                         'Code_Serviphar', 'Dénomination_Serviphar', 'Tarif_Serviphar',
                                         'Condition_commerciale_Serviphar',
                                         'Code_Soleomed', 'Dénomination_Soleomed', 'Tarif_Soleomed',
                                         'Code_Veso', 'Dénomination_Veso', 'Tarif_Veso'])

        # Search existing products in database
        query_products = text("""
            select p.id as produit_id, p.denomination as denomination_temp, p.conditionnement as conditionnement_temp, 
                p.laboratoire_id as laboratoire_id_temp, p.obsolete as obsolete_temp, p.invisible as invisible_temp, 
                p.famille_therapeutique_id as famille_therapeutique_id_temp, p.code_gtin
            from produits p
            join country c on c.id = :countryId
            where p.code_gtin is not null""")
        df_products = pd.read_sql_query(query_products, connection, params={"countryId": country_id})
        df_products['code_gtin'] = pd.to_numeric(df_products['code_gtin'])

        # Search existing codes sources of products in database
        query_product_sources = text("""
            select id as centrale_produit_id, code_produit, centrale_id, cirrina_pricing_condition_id
            from centrale_produit
            where country_id = :countryId
            and produit_id is not null""")
        df_product_sources = pd.read_sql_query(query_product_sources, connection, params={"countryId": country_id})
        df_product_sources['cirrina_pricing_condition_id'] = pd.to_numeric(
            df_product_sources['cirrina_pricing_condition_id'])

        # Search existing therapeutic classes in database
        query_classes = text("""
            select id as famille_therapeutique_id, CONCAT(classe1_code, coalesce(classe2_code, ''), 
                coalesce(classe3_code, '')) as famille_therapeutique
            from familles_therapeutiques""")
        df_classes = pd.read_sql_query(query_classes, connection)

        for source_id, df_group in df.groupby(['source_id']):
            source_id = int(source_id)
            source_name = common.get_name_of_source(connection, country_id, country_name, source_id, None).capitalize()
            logging.info(f"Source : {source_name}")

            df_temp = df_group.copy()
            df_temp['product_gtin'] = pd.to_numeric(df_temp['product_gtin'], errors="coerce")
            temp = pd.merge(df_temp[pd.notnull(df_temp['product_gtin'])], df_products,
                            left_on='product_gtin', right_on='code_gtin', how='left')
            df_source = pd.concat([temp, df_temp[pd.isnull(df_temp['product_gtin'])]], axis=0, sort=False,
                                  ignore_index=True)
            del temp

            df_source['denomination'] = df_source['product_name']
            df_source['obsolete'] = df_source['obsolete_temp']
            df_source['invisible'] = df_source['invisible_temp']
            df_source['conditionnement'] = np.nan
            df_source['types'] = np.nan
            df_source['especes'] = np.nan
            df_source['prix_unitaire'] = np.nan

            df_source['supplier'] = df_source['supplier'].dropna().apply(lambda x: x.strip())

            # Laboratories
            if source_id != constant.SOURCE_DIRECT_ID:
                # Search existing laboratories in database
                query_labs = text("""
                    select laboratoire_id, nom_laboratoire as laboratoire, cirrina_pricing_condition_id 
                    from centrale_laboratoire
                    where laboratoire_id is not null and centrale_id = :id""")
                df_labs = pd.read_sql_query(query_labs, connection, params={'id': source_id})
                df_labs['cirrina_pricing_condition_id'] = pd.to_numeric(df_labs['cirrina_pricing_condition_id'])

                df_source = pd.merge(df_source, df_labs, left_on=['supplier'], right_on=['laboratoire'], how='left')
            else:
                # Case of direct
                for supplier_name in df_source['supplier'].drop_duplicates():
                    supplier_id = common.get_id_of_source(connection, country_id, country_name, supplier_name)[1]
                    df_source.loc[(df_source['supplier'] == supplier_name), 'laboratoire_id'] = supplier_id
                df_source['cirrina_pricing_condition_id'] = np.nan

            df_source.loc[df_source['laboratoire_id'].isnull(), 'laboratoire_id'] = df_source['supplier']
            df_source['cirrina_pricing_condition_id'] = pd.to_numeric(df_source['cirrina_pricing_condition_id'])

            # Therapeutic classes
            if 'classe_therapeutique' not in df_source.columns:
                df_source['classe_therapeutique'] = np.nan
            df_source['famille_therapeutique'] = df_source['classe_therapeutique'].dropna().apply(
                lambda x: str(x).replace(' ', '')[:4])
            temp = pd.merge(df_source[pd.notnull(df_source['famille_therapeutique'])], df_classes,
                            on=['famille_therapeutique'], how='left')
            df_source = pd.concat([temp, df_source[pd.isnull(df_source['famille_therapeutique'])]], axis=0, sort=False,
                                  ignore_index=True)
            del temp

            # Check if source code already added
            if source_id in [18, 19]:
                df_source["product_code"] = df_source["product_code"].dropna().apply(lambda x: str(x))
            else:
                df_source["product_code"] = df_source["product_code"].dropna().apply(
                    lambda x: str(int(x)) if type(x) is float else str(x) if type(x) is int else x)
            df_source = pd.merge(df_source, df_product_sources.loc[(df_product_sources["centrale_id"] == source_id), :],
                                 left_on=['product_code', 'cirrina_pricing_condition_id'],
                                 right_on=['code_produit', 'cirrina_pricing_condition_id'],
                                 how='left', indicator=True)
            df_source = df_source.loc[(df_source['_merge'] != "both"), :]
            df_source.drop(columns=['_merge'], inplace=True)

            # Insert source code in database
            insert_source_code(df_source, source_id)

            columns = ['produit_id', 'denomination_temp', 'conditionnement_temp', 'laboratoire_id_temp',
                       'obsolete_temp', 'invisible_temp', 'famille_therapeutique_id_temp', 'product_gtin',
                       'classe_therapeutique', 'famille_therapeutique_id', 'denomination', 'conditionnement',
                       'laboratoire_id', 'obsolete', 'invisible', 'types', 'especes', 'product_code', 'product_name',
                       'prix_unitaire']
            columns_name = ['Id', 'Dénomination_temp', 'Conditionnement_temp', 'Laboratoire_temp', 'Obsolète_temp',
                            'Invisible_temp', 'ID classe thérapeutique_temp', 'Code GTIN', 'Autre code GTIN',
                            'ID classe thérapeutique', 'Dénomination', 'Conditionnement', 'Laboratoire',
                            'Obsolète', 'Invisible', 'Types', 'Espèces', 'Code_' + source_name,
                            'Dénomination_' + source_name, 'Tarif_' + source_name]

            if source_id in [18, 19]:
                columns.append('cirrina_pricing_condition_id')
                columns_name.append('Condition_commerciale_' + source_name)

            df_source = df_source[columns]
            df_source.columns = columns_name

            df_final = pd.concat([df_final, df_source.drop_duplicates()], axis=0, sort=False, ignore_index=True)

        # Sort dataframe
        df_final = df_final.sort_values(by=['Laboratoire', 'Code GTIN'])

        logging.debug("Generate files of new products")
        writer = pd.ExcelWriter(
            products_out_dir + now + "_" + country_name + "_new_products_purchases.xlsx",
            engine='xlsxwriter')
        df_final.to_excel(writer, sheet_name='Sheet 1', index=False)
        workbook = writer.book
        worksheet = writer.sheets['Sheet 1']
        red_format = workbook.add_format()
        red_format.set_bg_color('red')
        for col in ['R', 'U', 'X', 'AA', 'AD', 'AG', 'AJ', 'AM', 'AP', 'AS', 'AV', 'AY', 'BB', 'BE', 'BH', 'BK', 'BN',
                    'BR']:
            worksheet.conditional_format(col + '2:' + col + str(len(df_final.index)),
                                         {'type': 'duplicate', 'format': red_format})
        writer.save()
        shutil.copy(
            products_out_dir + now + "_" + country_name + "_new_products_purchases.xlsx",
            products_out_dir + now + "_" + country_name + "_new_products_purchases_source.xlsx")
    else:
        logging.debug(f'...no files to process !')

    logging.info(f"** Generate new products of purchases for country '{country_name}' end **")


def process_suppliers():
    logging.info(f"** Generate new suppliers of purchases for country '{country_name}' start **")

    suppliers_dir = root_dir + constant.DIR_SUPPLIERS + "/" + country_name + "/"
    suppliers_new_dir = suppliers_dir + constant.DIR_NEW + '/'
    suppliers_out_dir = suppliers_dir + constant.DIR_ARCHIVES + '/' + now + "_purchases/"
    suppliers_out_files_dir = suppliers_out_dir + constant.DIR_FILES + "/"

    file_exist = False
    for filename in os.listdir(suppliers_new_dir):
        if re.search('unknown_suppliers_*.*', filename):
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

        df['supplier'] = df['supplier'].dropna().apply(lambda x: x.strip())

        # Check if supplier already exists
        query = text("""   select id as centrale_laboratoire_id, nom_laboratoire as supplier, centrale_id as source_id
                            from centrale_laboratoire""")

        df = pd.merge(df, pd.read_sql_query(query, connection), on=['source_id', 'supplier'], how='left')
        df = df.loc[df['centrale_laboratoire_id'].isnull(), :]

        df = df[['source_id', 'supplier']]
        df.columns = ['centrale_id', 'nom_laboratoire']
        df.to_sql('centrale_laboratoire', engine, if_exists='append', index=False, chunksize=1000)
        logging.debug(f'Created {len(df.index)} new suppliers for sources')
    else:
        logging.debug(f'...no files to process !')

    logging.info(f"** Generate new suppliers of purchases for country '{country_name}' end **")


if __name__ == "__main__":
    # Getting args
    args = get_arguments()
    country_id = int(args.country)
    debug = args.debug

    now = datetime.now().strftime('%Y%m%d%H%M%S')

    # Initialize logging
    if debug:
        logging.basicConfig(format="%(asctime)s  %(levelname)s: %(message)s", level="DEBUG")
    else:
        params_logging = config(section="logging")
        logging.basicConfig(
            filename=params_logging["url"] + "/" + constant.LOG_PURCHASES_FILENAME + constant.LOG_EXTENSION,
            format="%(asctime)s  %(levelname)s: %(message)s", level=params_logging["level"])

    logging.info(f"Generating from purchases errors start !")

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

        country_name = common.get_name_of_country(connection, country_id)

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
