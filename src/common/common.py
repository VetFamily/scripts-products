import logging
import os
import shutil

from config import config
from src.common import constant


def get_id_of_country(name):
    for key, value in constant.countries.items():
        if value["name"] == name:
            return key

    return None


def get_name_of_country(country_id):
    return constant.countries[country_id]["name"]


def get_name_of_source(source_id, supplier_id):
    if supplier_id is not None:
        return constant.sources_direct[supplier_id]["name"]
    else:
        return constant.sources[source_id]["name"]


def get_id_of_source(name):
    for key, value in constant.sources.items():
        if value["name"] == name:
            return key, None

    for key, value in constant.sources_direct.items():
        if value["name"] == name:
            return constant.SOURCE_DIRECT_ID, key

    return None, None

