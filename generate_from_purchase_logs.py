# pip3 install pandas
# pip3 install xlrd


import argparse
import glob
import logging
import os
import re
import shutil
import string
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
    parser = argparse.ArgumentParser(
        description="Generation du fichier des nouveaux produits"
    )
    # parser.add_argument('-o', '--output', help='Output file name', default='stdout')
    required_args = parser.add_argument_group("required named arguments")
    required_args.add_argument("-c", "--country", help="ID of country", required=True)
    optional_args = parser.add_argument_group("optional named arguments")
    optional_args.add_argument(
        "-d", "--debug", help="Logging debug in console", action="store_true"
    )
    return parser.parse_args()


def insert_source_code(df, source_id):
    df_source_tmp = df.copy()
    date = pd.to_datetime("2016-01-01", format="%Y-%m-%d", errors="coerce")

    del df_source_tmp["centrale_produit_id"]
    query_product_sources = text(
        """
        select id as centrale_produit_id, code_produit as product_code, cirrina_pricing_condition_id,
            supplier_id as laboratoire_id
        from centrale_produit
        where centrale_id = :sourceId
        and country_id = :countryId"""
    )
    df_tmp = pd.read_sql_query(
        query_product_sources,
        connection,
        params={"sourceId": source_id, "countryId": country_id},
    )
    df_tmp["cirrina_pricing_condition_id"] = pd.to_numeric(
        df_tmp["cirrina_pricing_condition_id"]
    )
    df_tmp["laboratoire_id"] = pd.to_numeric(df_tmp["laboratoire_id"])

    if source_id not in [constant.SOURCE_DIRECT_ID, constant.SOURCE_HEILAND_ID]:
        df_source_tmp = pd.merge(
            df_source_tmp,
            df_tmp.drop(columns=["laboratoire_id"]),
            on=["product_code", "cirrina_pricing_condition_id"],
            how="left",
        )
        df_source_tmp = df_source_tmp[df_source_tmp["centrale_produit_id"].isnull()]
        del df_tmp

        # Insert into centrale_produit
        df_temp_new_cp = df_source_tmp[
            ["product_code", "cirrina_pricing_condition_id"]
        ].copy()
        df_temp_new_cp.rename(columns={"product_code": "code_produit"}, inplace=True)
        df_temp_new_cp["centrale_id"] = source_id
        df_temp_new_cp["country_id"] = country_id
        df_temp_new_cp.drop_duplicates(inplace=True)
        df_temp_new_cp.to_sql(
            "centrale_produit", engine, if_exists="append", index=False, chunksize=1000
        )
        logging.debug(
            f"Table centrale_produit : {len(df_temp_new_cp)} elements created"
        )

        # Search existing codes sources of products in database
        del df_source_tmp["centrale_produit_id"]
        df_tmp = pd.read_sql_query(
            query_product_sources,
            connection,
            params={"sourceId": source_id, "countryId": country_id},
        )
        df_tmp["cirrina_pricing_condition_id"] = pd.to_numeric(
            df_tmp["cirrina_pricing_condition_id"]
        )
        df_source_tmp = pd.merge(
            df_source_tmp,
            df_tmp.drop(columns=["laboratoire_id"]),
            on=["product_code", "cirrina_pricing_condition_id"],
            how="left",
        )
    else:
        df_source_tmp = pd.merge(
            df_source_tmp,
            df_tmp,
            on=["product_code", "cirrina_pricing_condition_id", "laboratoire_id"],
            how="left",
        )
        df_source_tmp = df_source_tmp[df_source_tmp["centrale_produit_id"].isnull()]
        del df_tmp

        # Insert into centrale_produit
        df_temp_new_cp = df_source_tmp[
            ["product_code", "cirrina_pricing_condition_id", "laboratoire_id"]
        ].copy()
        df_temp_new_cp.rename(
            columns={"product_code": "code_produit", "laboratoire_id": "supplier_id"},
            inplace=True,
        )
        df_temp_new_cp["centrale_id"] = source_id
        df_temp_new_cp["country_id"] = country_id
        df_temp_new_cp.drop_duplicates(inplace=True)
        df_temp_new_cp.to_sql(
            "centrale_produit", engine, if_exists="append", index=False, chunksize=1000
        )
        logging.debug(
            f"Table centrale_produit : {len(df_temp_new_cp)} elements created"
        )

        # Search existing codes sources of products in database
        del df_source_tmp["centrale_produit_id"]
        df_tmp = pd.read_sql_query(
            query_product_sources,
            connection,
            params={"sourceId": source_id, "countryId": country_id},
        )
        df_tmp["cirrina_pricing_condition_id"] = pd.to_numeric(
            df_tmp["cirrina_pricing_condition_id"]
        )
        df_source_tmp = pd.merge(
            df_source_tmp,
            df_tmp,
            on=["product_code", "cirrina_pricing_condition_id", "laboratoire_id"],
            how="left",
        )
    del df_tmp

    if len(df_source_tmp) > 0:
        # Insert into centrale_produit_denominations
        df_temp_new_cpd = df_source_tmp[["centrale_produit_id", "product_name"]].copy()
        df_temp_new_cpd.rename(columns={"product_name": "nom"}, inplace=True)
        df_temp_new_cpd["date_creation"] = date
        df_temp_new_cpd.drop_duplicates(inplace=True)
        df_temp_new_cpd.to_sql(
            "centrale_produit_denominations",
            engine,
            if_exists="append",
            index=False,
            chunksize=1000,
        )
        logging.debug(
            f"Table centrale_produit_denominations : {len(df_temp_new_cpd)} elements created"
        )

        del df_temp_new_cpd

    del df_source_tmp, df_temp_new_cp


def add_packaging(df, country_id):

    if df.empty:
        logging.debug("Empty dataframe")

        return

    # Open the file with packaging & alternative names and turn it into dic

    path_ed_packaging = "data/ed_packaging.xlsx"
    df_type_packaging = pd.read_excel(path_ed_packaging)
    df_type_packaging = df_type_packaging.T
    df_type_packaging.reset_index(inplace=True)
    df_type_packaging.columns = df_type_packaging.iloc[0]
    df_type_packaging = df_type_packaging[1:]

    units_dict = {}
    units = []

    for _, row in df_type_packaging.iterrows():
        pack_id = row["pack_id"]
        pack_names = row.dropna()[1:].tolist()
        units_dict[pack_id] = pack_names
        units.extend(pack_names)

    # Récupération des séparateurs
    path_fichier_separateurs = "data/separateurs_list.xlsx"
    df_separateurs = pd.read_excel(path_fichier_separateurs)
    separateurs_list = df_separateurs["separateurs"].tolist()

    # the following pattern detect all the packaging with number + units
    # the number can be separate by a separateur (12 X 4 units)
    # the pattern catch different format of number : (10000, 10 000, 10000,0)
    pattern = (
        r"\b((?:\d+(?:\s?\d{3})*(?:[.,]\d+)?)?(?:\s*(?:"
        + "|".join(map(re.escape, separateurs_list))
        + r")\s*(?:\d+(?:\s?\d{3})*(?:[.,]\d+)?))*)\s*("
        + "|".join(map(re.escape, units))
        + r")(?:\s*\.\s*)?\b"
    )

    # Multiplication des valeurs si un séparateur est présent (ex : 2 x 120 g, ou 3 boîtes de 2 kg)
    def compute_multiplication(value, separateurs_list):
        if any(separateur.lower() in value.lower() for separateur in separateurs_list):
            for separateur in separateurs_list:
                value = value.replace(separateur, "x")

            parts = value.split("x")

            try:
                result = 1
                for part in parts:
                    result *= float(part.replace(",", "."))
                result = str(result)
                return result

            except ValueError:
                return None
        else:
            return value

    # Fonction pour appliquer la logique à une seule ligne
    def extract_packaging(product):
        matches = re.findall(pattern, product, flags=re.IGNORECASE)
        if matches:
            value, unit = matches[-1]
            value = re.sub(r"(?<=\d)\s+(?=\d)", "", value)  # Format digits (no space)
            type_packaging = next(
                (
                    key
                    for key, values in units_dict.items()
                    if unit.lower() in map(str.lower, values)
                ),
                999,
            )
            # Get the first element to get the good unit
            full_pack = str(value + " " + units_dict[type_packaging][0])

            # get the value by multiply in case of 3 x 4 kg
            value = compute_multiplication(value, separateurs_list)
            return pd.Series([full_pack, value, type_packaging])
        else:
            return pd.Series([None, None, None])

    if country_id == 1:
        # Don't fill conditionnement columns for France : Do it manually for now for better data quality
        df[["value_packaging", "type_packaging"]] = (
            df["denomination"].apply(extract_packaging).iloc[:, [1, 2]]
        )
    else:
        df[["conditionnement", "value_packaging", "type_packaging"]] = df[
            "denomination"
        ].apply(extract_packaging)


def process_products():
    logging.info(
        f"** Generate new products of purchases for country '{country_name}' start **"
    )

    products_dir = os.path.join(root_dir, constant.DIR_PRODUCTS, country_name)
    products_new_dir = os.path.join(products_dir, constant.DIR_NEW)
    products_out_dir = os.path.join(
        products_dir, constant.DIR_ARCHIVES, now + "_purchases"
    )
    products_out_files_dir = os.path.join(products_out_dir, constant.DIR_FILES)

    file_exist = False
    for filename in os.listdir(products_new_dir):
        if re.search("unknown_products_*.*", filename):
            file_exist = True
            break

    # stop function early if no files
    if not file_exist:
        logging.debug("...no files to process !")
        logging.info(
            f"** Generate new products of purchases for country '{country_name}' end **"
        )
        return

    # Create directories if not exist
    os.makedirs(products_out_files_dir, exist_ok=True)

    for f in sorted(glob.glob(os.path.join(products_new_dir, "unknown_products_*.*"))):
        filename = os.path.basename(f)
        logging.debug(f'** Move "{filename}" to backup files directory **')
        shutil.move(f, os.path.join(products_out_files_dir, filename))

    # Initialize dataframe of products, making sure that there is a 'product_type' column
    df = pd.DataFrame(columns=["product_type"])

    logging.debug("Aggregate files...")
    for f in sorted(glob.glob(os.path.join(products_out_files_dir, "*.*"))):
        df = pd.concat(
            [df, pd.read_excel(f, dtype=str)], axis=0, sort=False, ignore_index=True
        )

    # Save dataframe for logs
    df.to_excel(
        os.path.join(products_out_dir, now + "_unknown_products_aggregated.xlsx"),
        index=False,
        engine="xlsxwriter",
    )
    logging.debug("Aggregated file is generated !")

    # Drop duplicates
    df.drop_duplicates(inplace=True)

    # Ignore rows without product code
    df = df[df["product_code"].notnull()]

    df_final = pd.DataFrame(
        columns=[
            "Id",
            "Dénomination_temp",
            "Conditionnement_temp",
            "Laboratoire_temp",
            "Obsolète_temp",
            "Invisible_temp",
            "ID classe thérapeutique_temp",
            "Code GTIN",
            "Autre code GTIN",
            "ID classe thérapeutique",
            "Dénomination",
            "Conditionnement",
            "Value packaging",
            "Type packaging",
            "Laboratoire",
            "Obsolète",
            "Invisible",
            "Types",
            "Espèces",
        ]
    )

    # Search existing products in database
    query_products = """
        select p.id as produit_id, p.denomination as denomination_temp, p.conditionnement as conditionnement_temp, p.value_packaging, p.type_packaging,
            p.laboratoire_id as laboratoire_id_temp, p.obsolete as obsolete_temp, p.invisible as invisible_temp,
            p.famille_therapeutique_id as famille_therapeutique_id_temp, p.code_gtin
        from produits p"""
    df_products = pd.read_sql_query(query_products, connection)

    # Search existing codes sources of products in database
    query_product_sources = """
        select id as centrale_produit_id, code_produit, centrale_id, cirrina_pricing_condition_id,
               produit_id, country_id, supplier_id as laboratoire_id
        from centrale_produit
        where produit_id is not null"""
    df_product_sources = pd.read_sql_query(query_product_sources, connection)
    df_product_sources["cirrina_pricing_condition_id"] = pd.to_numeric(
        df_product_sources["cirrina_pricing_condition_id"]
    )
    df_product_sources["laboratoire_id"] = pd.to_numeric(
        df_product_sources["laboratoire_id"]
    )

    # Search existing therapeutic classes in database
    query_classes = text(
        """
        select id as famille_therapeutique_id, CONCAT(classe1_code, coalesce(classe2_code, ''),
            coalesce(classe3_code, '')) as famille_therapeutique
        from familles_therapeutiques"""
    )
    df_classes = pd.read_sql_query(query_classes, connection)

    types_especes = pd.read_excel("data/Types_Especes.xlsx")
    if types_especes.duplicated(
        subset=["source_id", "supplier_id", "type_source"]
    ).any():
        raise ValueError(
            "Types_Especes.xlsx contains duplicated rows: "
            "same source_id, same supplier_id, same type_source."
        )

    for source_id, df_group in df.groupby(["source_id"]):
        source_id = int(source_id)
        source_name = common.get_name_of_source(
            connection, country_id, country_name, source_id, None
        ).capitalize()
        logging.info(f"Source : {source_name}")

        df_temp = df_group.copy()
        df_products_temp = df_products.copy()

        if source_id not in [constant.SOURCE_CIRRINA_ID, constant.SOURCE_HEILAND_ID]:
            df_products_temp["code_gtin"] = pd.to_numeric(
                df_products_temp["code_gtin"], errors="coerce"
            )
            df_temp["product_gtin"] = pd.to_numeric(
                df_temp["product_gtin"], errors="coerce"
            )

        # Laboratories
        if source_id not in [constant.SOURCE_DIRECT_ID]:
            # Search existing laboratories in database
            query_labs = text(
                """
                select laboratoire_id, nom_laboratoire as laboratoire, cirrina_pricing_condition_id
                from centrale_laboratoire
                where laboratoire_id is not null and centrale_id = :id"""
            )
            df_labs = pd.read_sql_query(
                query_labs, connection, params={"id": source_id}
            )
            df_labs["cirrina_pricing_condition_id"] = pd.to_numeric(
                df_labs["cirrina_pricing_condition_id"]
            )

            df_temp = pd.merge(
                df_temp, df_labs, left_on="supplier", right_on="laboratoire", how="left"
            )
        else:
            # Case of direct
            for supplier_name in df_temp["supplier"].drop_duplicates():
                supplier_id = common.get_id_of_source(
                    connection, country_id, country_name, supplier_name
                )[1]
                df_temp.loc[df_temp["supplier"] == supplier_name, "laboratoire_id"] = (
                    supplier_id
                )
            df_temp["cirrina_pricing_condition_id"] = np.nan

        df_temp.loc[df_temp["laboratoire_id"].isnull(), "laboratoire_id"] = df_temp[
            "supplier"
        ]
        df_temp["cirrina_pricing_condition_id"] = pd.to_numeric(
            df_temp["cirrina_pricing_condition_id"]
        )

        # Therapeutic classes
        if "classe_therapeutique" not in df_temp.columns:
            df_temp["classe_therapeutique"] = np.nan
        df_temp["famille_therapeutique"] = (
            df_temp["classe_therapeutique"]
            .dropna()
            .apply(lambda x: str(x).replace(" ", "")[:4])
            .astype(str)
        )

        df_temp = pd.merge(
            df_temp,
            df_classes[df_classes["famille_therapeutique"].notnull()],
            how="left",
            on="famille_therapeutique",
        )

        # add temporary id to make sequence of lookups easier
        df_temp["temp_id"] = range(len(df_temp))

        # first lookup using GTIN
        df_source = pd.merge(
            df_temp,
            df_products_temp[df_products_temp["code_gtin"].notnull()],
            how="inner",
            left_on="product_gtin",
            right_on="code_gtin",
        )

        # add temporary id to make sequence of lookups easier
        df_temp["temp_id"] = range(len(df_temp))

        # first lookup using GTIN
        df_source = pd.merge(
            df_temp,
            df_products_temp[df_products_temp["code_gtin"].notnull()],
            how="inner",
            left_on="product_gtin",
            right_on="code_gtin",
        )

        # when no match: new lookup using product code (only for source_id DIRECT and KRUUSE)
        if source_id in [
            constant.SOURCE_DIRECT_ID,
            constant.SOURCE_HEILAND_ID,
            constant.SOURCE_KRUUSE_ID,
        ]:
            df_temp_not_matched = df_temp[
                ~df_temp["temp_id"].isin(df_source["temp_id"])
            ]

            df_product_codes = df_product_sources[
                df_product_sources["centrale_id"] == source_id
            ]
            df_product_codes = df_product_codes[
                ["code_produit", "produit_id", "laboratoire_id"]
            ]

            df_source2 = pd.merge(
                df_temp_not_matched,
                df_product_codes[df_product_codes["code_produit"].notnull()],
                how="inner",
                left_on=["product_code", "laboratoire_id"],
                right_on=["code_produit", "laboratoire_id"],
            )

            df_source2 = pd.merge(
                df_source2, df_products_temp, how="inner", on="produit_id"
            )

            df_source2.drop(columns=["code_produit"], inplace=True)
            df_source = pd.concat(
                [df_source, df_source2], axis=0, sort=False, ignore_index=True
            )

        # when still no match: new lookup using product name
        df_temp_not_matched = df_temp[
            ~df_temp["temp_id"].isin(df_source["temp_id"])
        ].copy()

        df_temp_not_matched["normalized_name"] = df_temp_not_matched[
            "product_name"
        ].str.lower()
        df_products_temp["normalized_name"] = df_products_temp[
            "denomination_temp"
        ].str.lower()

        df_source3 = pd.merge(
            df_temp_not_matched,
            df_products_temp[df_products_temp["denomination_temp"].notnull()],
            how="inner",
            on="normalized_name",
        )
        del df_source3["normalized_name"], df_products_temp

        df_source = pd.concat(
            [df_source, df_source3], axis=0, sort=False, ignore_index=True
        )

        # add unmatched rows to df_source
        df_temp_not_matched = df_temp[
            ~df_temp["temp_id"].isin(df_source["temp_id"])
        ].copy()
        df_source = pd.concat(
            [df_source, df_temp_not_matched], axis=0, sort=False, ignore_index=True
        )

        del df_temp_not_matched

        # in case there were multiple matches, keep only the first one
        df_source = df_source.groupby("temp_id", as_index=False).nth(0)

        del df_source["temp_id"]

        df_source["denomination"] = df_source["product_name"]
        df_source["obsolete"] = np.where(
            df_source["obsolete_temp"].notnull(), df_source["obsolete_temp"], False
        )
        df_source["invisible"] = np.where(
            df_source["invisible_temp"].notnull(), df_source["invisible_temp"], False
        )
        df_source["conditionnement"] = np.nan
        df_source["types"] = np.nan
        df_source["especes"] = np.nan

        df_source["supplier"] = df_source["supplier"].dropna().str.strip()

        # Check if source code already added
        if source_id in [
            constant.SOURCE_CIRRINA_ID,
            constant.SOURCE_SERVIPHAR_ID,
            constant.SOURCE_DISTRIVET_ID,
        ]:
            df_source["product_code"] = df_source["product_code"].dropna().astype(str)
        else:
            df_source["product_code"] = (
                df_source["product_code"]
                .dropna()
                .apply(
                    lambda x: (
                        str(int(x))
                        if type(x) is float
                        else str(x) if type(x) is int else x
                    )
                )
            )
        df_source = pd.merge(
            df_source,
            df_product_sources[
                (df_product_sources["centrale_id"] == source_id)
                & (df_product_sources["country_id"] == country_id)
            ].drop(
                columns=(
                    ["produit_id", "country_id", "laboratoire_id"]
                    if source_id
                    not in [constant.SOURCE_DIRECT_ID, constant.SOURCE_HEILAND_ID]
                    else ["produit_id", "country_id"]
                )
            ),
            left_on=(
                ["product_code", "cirrina_pricing_condition_id"]
                if source_id
                not in [constant.SOURCE_DIRECT_ID, constant.SOURCE_HEILAND_ID]
                else ["product_code", "cirrina_pricing_condition_id", "laboratoire_id"]
            ),
            right_on=(
                ["code_produit", "cirrina_pricing_condition_id"]
                if source_id
                not in [constant.SOURCE_DIRECT_ID, constant.SOURCE_HEILAND_ID]
                else ["code_produit", "cirrina_pricing_condition_id", "laboratoire_id"]
            ),
            how="left",
            indicator=True,
        )
        df_source = df_source[df_source["_merge"] != "both"]
        del df_source["_merge"]

        # Insert source code in database
        insert_source_code(df_source, source_id)

        df_source["denomination"] = df_source["denomination"].str.upper()

        # for countries other than France:
        if country_id != 1:
            df_source["famille_therapeutique_id"] = 233

        # auto-complete types and species
        df_source_copy = df_source.copy()
        df_source_copy["laboratoire_id"] = pd.to_numeric(
            df_source_copy["laboratoire_id"], errors="coerce"
        )

        types_especes_restricted = types_especes[
            types_especes["source_id"] == source_id
        ].copy()
        types_especes_restricted.rename(
            columns={"type": "new_type", "especes": "new_especes"}, inplace=True
        )

        if (
            len(
                types_especes_restricted[
                    types_especes_restricted["type_source"].notnull()
                ]
            )
            > 0
        ):
            df_source_merged = (
                df_source_copy.reset_index()
                .merge(
                    types_especes_restricted[
                        types_especes_restricted["type_source"].notnull()
                    ],
                    how="left",
                    left_on=(
                        ["laboratoire_id", "product_type"]
                        if source_id
                        in [constant.SOURCE_DIRECT_ID, constant.SOURCE_HEILAND_ID]
                        else "product_type"
                    ),
                    right_on=(
                        ["supplier_id", "type_source"]
                        if source_id
                        in [constant.SOURCE_DIRECT_ID, constant.SOURCE_HEILAND_ID]
                        else "type_source"
                    ),
                )
                .set_index("index")
            )

            df_source["types"] = df_source_merged["new_type"]
            df_source["especes"] = df_source_merged["new_especes"]

        if (
            len(
                types_especes_restricted[
                    types_especes_restricted["type_source"].isnull()
                ]
            )
            > 0
        ):
            if source_id in [constant.SOURCE_DIRECT_ID, constant.SOURCE_HEILAND_ID]:
                df_source_merged = (
                    df_source_copy.reset_index()
                    .merge(
                        types_especes_restricted[
                            types_especes_restricted["type_source"].isnull()
                        ],
                        how="left",
                        left_on=["laboratoire_id"],
                        right_on=["supplier_id"],
                    )
                    .set_index("index")
                )
                df_source["types"] = df_source_merged["new_type"]
                df_source["especes"] = df_source_merged["new_especes"]
            else:
                df_source["types"] = types_especes_restricted["new_type"].iloc[0]
                df_source["especes"] = types_especes_restricted["new_especes"].iloc[0]

        # Specific steps for distributor/supplier
        """if source_id == constant.SOURCE_APOEX_ID:
            df_source['denomination'] = np.where(df_source['product_type'] == '...',
                                                 df_source['denomination'] + 'MerFM', df_source['denomination'])
        elif source_id == constant.SOURCE_DIRECT_ID:
            for supplier_id in df_source['laboratoire_id'].drop_duplicates():
                # Write specific step for df_source[df_source['laboratoire_id'] == supplier_id]
                """

        columns = [
            "produit_id",
            "denomination_temp",
            "conditionnement_temp",
            "laboratoire_id_temp",
            "obsolete_temp",
            "invisible_temp",
            "famille_therapeutique_id_temp",
            "product_gtin",
            "classe_therapeutique",
            "famille_therapeutique_id",
            "denomination",
            "conditionnement",
            "value_packaging",
            "type_packaging",
            "laboratoire_id",
            "obsolete",
            "invisible",
            "types",
            "especes",
            "product_code",
            "product_name",
        ]

        columns_name = [
            "Id",
            "Dénomination_temp",
            "Conditionnement_temp",
            "Laboratoire_temp",
            "Obsolète_temp",
            "Invisible_temp",
            "ID classe thérapeutique_temp",
            "Code GTIN",
            "Autre code GTIN",
            "ID classe thérapeutique",
            "Dénomination",
            "Conditionnement",
            "Value packaging",
            "Type packaging",
            "Laboratoire",
            "Obsolète",
            "Invisible",
            "Types",
            "Espèces",
            "Code_" + source_name,
            "Dénomination_" + source_name,
        ]

        if source_id in [constant.SOURCE_CIRRINA_ID, constant.SOURCE_SERVIPHAR_ID]:
            columns.append("cirrina_pricing_condition_id")
            columns_name.append("Condition_commerciale_" + source_name)

        if source_id in [constant.SOURCE_DIRECT_ID, constant.SOURCE_HEILAND_ID]:
            df_source["supplier_id"] = df_source["laboratoire_id"]
            columns.append("supplier_id")
            columns_name.append("Laboratoire_" + source_name)

        df_source = df_source[columns]

        # extract & add value and type packaging
        add_packaging(df_source, country_id)
        df_source["value_packaging"] = df_source["value_packaging"].str.replace(
            ",", ".", regex=False
        )
        df_source["value_packaging"] = df_source["value_packaging"].astype("float")

        # remove empty 'useless' columns
        for col in ["product_code", "product_name", "cirrina_pricing_condition_id"]:
            if col in df_source.columns and df_source[col].isnull().all():
                del df_source[col]

        df_source.rename(columns=dict(zip(columns, columns_name)), inplace=True)

        df_final = pd.concat(
            [df_final, df_source.drop_duplicates()],
            axis=0,
            sort=False,
            ignore_index=True,
        )

    # Sort dataframe, keeping empty Id's at the top
    df_final["nonempty_id"] = df_final["Id"].notnull()
    df_final.sort_values(by=["nonempty_id", "Laboratoire", "Code GTIN"], inplace=True)
    del df_final["nonempty_id"]

    logging.debug("Generate files of new products")
    writer = pd.ExcelWriter(
        os.path.join(
            products_out_dir, now + "_" + country_name + "_new_products_purchases.xlsx"
        ),
        engine="xlsxwriter",
    )
    df_final.to_excel(writer, sheet_name="Sheet 1", index=False)
    workbook = writer.book
    worksheet = writer.sheets["Sheet 1"]
    red_format = workbook.add_format()
    red_format.set_bg_color("red")

    # all Excel columns from A to CZ
    alphabet = list(string.ascii_uppercase)
    excel_cols = (
        alphabet
        + ["A" + x for x in alphabet]
        + ["B" + x for x in alphabet]
        + ["C" + x for x in alphabet]
    )
    # Excel names of all columns beginning with 'Code_'
    code_cols = [
        excel_cols[i]
        for i in range(len(df_final.columns))
        if df_final.columns[i].startswith("Code_")
        or (df_final.columns[i] == "Dénomination")
    ]

    for col in code_cols:
        worksheet.conditional_format(
            col + "2:" + col + str(len(df_final) + 1),
            {"type": "duplicate", "format": red_format},
        )
    writer.save()
    shutil.copy(
        os.path.join(
            products_out_dir, now + "_" + country_name + "_new_products_purchases.xlsx"
        ),
        os.path.join(
            products_out_dir,
            now + "_" + country_name + "_new_products_purchases_source.xlsx",
        ),
    )

    logging.info(
        f"** Generate new products of purchases for country '{country_name}' end **"
    )


def process_suppliers():
    logging.info(
        f"** Generate new suppliers of purchases for country '{country_name}' start **"
    )

    suppliers_dir = os.path.join(root_dir, constant.DIR_SUPPLIERS, country_name)
    suppliers_new_dir = os.path.join(suppliers_dir, constant.DIR_NEW)
    suppliers_out_dir = os.path.join(
        suppliers_dir, constant.DIR_ARCHIVES, now + "_purchases"
    )
    suppliers_out_files_dir = os.path.join(suppliers_out_dir, constant.DIR_FILES)

    file_exist = False
    for filename in os.listdir(suppliers_new_dir):
        if re.search("unknown_suppliers_*.*", filename):
            file_exist = True
            break

    if file_exist:
        # Create directories if not exist
        os.makedirs(suppliers_out_files_dir, exist_ok=True)

        for f in sorted(glob.glob(os.path.join(suppliers_new_dir, "*.*"))):
            filename = os.path.basename(f)
            logging.debug(f'** Move "{filename}" to backup files directory **')
            shutil.move(f, os.path.join(suppliers_out_files_dir, filename))

        # Initialize dataframe of suppliers
        df = pd.DataFrame()

        logging.debug("Aggregate files...")
        for f in sorted(glob.glob(os.path.join(suppliers_out_files_dir, "*.*"))):
            df = pd.concat(
                [df, pd.read_excel(f)], axis=0, sort=False, ignore_index=True
            )

        df.drop_duplicates(inplace=True)

        # Save dataframe for logs
        df.to_excel(
            os.path.join(suppliers_out_dir, now + "_unknown_suppliers_aggregated.xlsx"),
            index=False,
            engine="xlsxwriter",
        )
        logging.debug("Aggregated file is generated !")

        df["supplier"] = df["supplier"].dropna().str.strip()

        # Check if supplier already exists
        query = text(
            """   select id as centrale_laboratoire_id, nom_laboratoire as supplier, centrale_id as source_id
                            from centrale_laboratoire"""
        )

        df = pd.merge(
            df,
            pd.read_sql_query(query, connection),
            on=["source_id", "supplier"],
            how="left",
        )
        df = df[df["centrale_laboratoire_id"].isnull()]

        df = df[["source_id", "supplier"]]
        df.rename(
            columns={"source_id": "centrale_id", "supplier": "nom_laboratoire"},
            inplace=True,
        )
        df.to_sql(
            "centrale_laboratoire",
            engine,
            if_exists="append",
            index=False,
            chunksize=1000,
        )
        logging.debug(f"Created {len(df)} new suppliers for sources")
    else:
        logging.debug("...no files to process !")

    logging.info(
        f"** Generate new suppliers of purchases for country '{country_name}' end **"
    )


if __name__ == "__main__":
    # Getting args
    args = get_arguments()
    country_id = int(args.country)
    debug = args.debug

    now = datetime.now().strftime("%Y%m%d%H%M%S")

    # Initialize logging
    if debug:
        logging.basicConfig(
            format="%(asctime)s  %(levelname)s: %(message)s", level="DEBUG"
        )
    else:
        params_logging = config(section="logging")
        logging.basicConfig(
            filename=os.path.join(
                params_logging["url"],
                constant.LOG_PURCHASES_FILENAME + constant.LOG_EXTENSION,
            ),
            format="%(asctime)s  %(levelname)s: %(message)s",
            level=params_logging["level"],
        )

    logging.info("Generating from purchases errors start !")

    try:
        # Reading parameters of directory
        params_dir = config(section="directories")
        root_dir = params_dir["root_dir"]

        # Reading parameters of database
        params_db = config()

        # Connecting to the PostgreSQL server
        logging.debug("Connecting to the PostgreSQL database...")
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
            logging.info("Generating from purchases errors end !")
