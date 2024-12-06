import logging
import re
import pandas as pd


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
            if value:
                return pd.Series([full_pack, value, type_packaging])
            else:
                return pd.Series([None, None, None])
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
