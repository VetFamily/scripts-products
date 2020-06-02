
# Countries
COUNTRY_FRANCE_ID = 1
COUNTRY_NETHERLANDS_ID = 2

countries = {
    COUNTRY_FRANCE_ID: {
        "name": "france"
    },
    COUNTRY_NETHERLANDS_ID: {
        "name": "netherlands"
    }
}

# Directories
DIR_PRODUCTS = "products"
DIR_SUPPLIERS = "suppliers"
DIR_NEW = "new"
DIR_ARCHIVES = "archives"
DIR_FILES = "files"

# Log
LOG_PURCHASES_FILENAME = "products-purchases"
LOG_EXTENSION = ".log"

# Suppliers
SUPPLIER_AUDEVARD_ID = 1
SUPPLIER_AXIENCE_ID = 2
SUPPLIER_BAYER_ID = 3
SUPPLIER_BIMEDA_ID = 4
SUPPLIER_BIOVE_ID = 5
SUPPLIER_BOEHRINGER_ID = 6
SUPPLIER_BOIRON_ID = 7
SUPPLIER_CEVA_ID = 8
SUPPLIER_DOPHARMA_COOPHAVET_ID = 9
SUPPLIER_DECHRA_ID = 10
SUPPLIER_ELANCO_ID = 11
SUPPLIER_HILLS_ID = 12
SUPPLIER_HIPRA_ID = 13
SUPPLIER_MERIAL_ID = 14
SUPPLIER_MP_LABO_ID = 15
SUPPLIER_MSD_ID = 16
SUPPLIER_NESTLE_PURINA_ID = 17
SUPPLIER_OSALIA_ID = 18
SUPPLIER_HUVEPHARMA_QALIAN_ID = 19
SUPPLIER_ROYAL_CANIN_ID = 20
SUPPLIER_SAVETIS_ID = 21
SUPPLIER_TVM_ID = 22
SUPPLIER_VETOQUINOL_ID = 23
SUPPLIER_VIRBAC_ID = 24
SUPPLIER_ZOETIS_ID = 25
SUPPLIER_POMMIER_ID = 26
SUPPLIER_GENIA_ID = 27
SUPPLIER_OBIONE_ID = 28
SUPPLIER_VETALIS_ID = 29
SUPPLIER_OPTINAC_ID = 31
SUPPLIER_LABO_IDT_ID = 32
SUPPLIER_LABORATOIRE_LCV_ID = 33
SUPPLIER_CEVA_BIOVAC_ID = 34
SUPPLIER_FILAVIE_ID = 35
SUPPLIER_CERTIVET_ID = 36
SUPPLIER_GREENPEX_ID = 37
SUPPLIER_SYNHERGI_ID = 38
SUPPLIER_LDCA_ID = 39
SUPPLIER_ALIVIRA_ID = 41
SUPPLIER_PHYSAN_ID = 42
SUPPLIER_NOVACTIV_ID = 43
SUPPLIER_CODICO_TONIVET_ID = 44
SUPPLIER_SV_ALCYON_ID = 45
SUPPLIER_TECHNOVET_ID = 47
SUPPLIER_CALIBRA_ID = 48
SUPPLIER_BIOTOPIS_ID = 49
SUPPLIER_WAMINE_ID = 50
SUPPLIER_KRKA_ID = 51
SUPPLIER_MILOA_ID = 52
SUPPLIER_ARCANATURA_ID = 53
SUPPLIER_BD_ANIMALHEALTH_ID = 54
SUPPLIER_BBRAUN_ID = 55
SUPPLIER_REBATE_ID = 56

# Sources
SOURCE_MULTI_ID = 0
SOURCE_ALCYON_ID = 1
SOURCE_CENTRAVET_ID = 2
SOURCE_COVETO_ID = 3
SOURCE_ALIBON_ID = 4
SOURCE_VETAPRO_ID = 5
SOURCE_VETYSPHARMA_ID = 6
SOURCE_HIPPOCAMPE_ID = 7
SOURCE_AGRIPHARM_ID = 8
SOURCE_ELVETIS_ID = 9
SOURCE_LONGIMPEX_ID = 10
SOURCE_DIRECT_ID = 11
SOURCE_CEDIVET_ID = 12
SOURCE_COVETRUS_ID = 13

sources = {
    SOURCE_MULTI_ID: {
        "name": "multi"
    },
    SOURCE_ALCYON_ID: {
        "name": "alcyon",
        "nb_of_columns": 13,
        "loc_clinic_code": 0,
        "loc_clinic_name": 1,
        "loc_product_code": 3,
        "loc_product_name": 4,
        "loc_product_supplier": 2,
        "loc_product_gtin": 7,
        "loc_product_vat": 5,
        "loc_paid_quantity": 9,
        "loc_free_quantity": 10,
        "loc_amount": 11,
        "loc_month": 8,
        "loc_year": None
    },
    SOURCE_CENTRAVET_ID: {
        "name": "centravet",
        "nb_of_columns": 19,
        "loc_clinic_code": 15,
        "loc_clinic_name": 1,
        "loc_product_code": 2,
        "loc_product_name": 5,
        "loc_product_supplier": 6,
        "loc_product_gtin": 4,
        "loc_product_vat": None,
        "loc_paid_quantity": 13,
        "loc_free_quantity": 14,
        "loc_amount": 12,
        "loc_month": 10,
        "loc_year": 11,
    },
    SOURCE_COVETO_ID: {
        "name": "coveto",
        "nb_of_columns": 13,
        "loc_clinic_code": 0,
        "loc_clinic_name": 1,
        "loc_product_code": 3,
        "loc_product_name": 4,
        "loc_product_supplier": 2,
        "loc_product_gtin": 7,
        "loc_product_vat": 5,
        "loc_paid_quantity": 9,
        "loc_free_quantity": 12,
        "loc_amount": 11,
        "loc_month": 8,
        "loc_year": None
    },
    SOURCE_ALIBON_ID: {
        "name": "alibon",
        "nb_of_columns": 10,
        "loc_clinic_code": 0,
        "loc_clinic_name": 1,
        "loc_product_code": 2,
        "loc_product_name": 7,
        "loc_product_supplier": 4,
        "loc_product_gtin": 3,
        "loc_product_vat": None,
        "loc_paid_quantity": 8,
        "loc_free_quantity": None,
        "loc_amount": 9,
        "loc_month": 6,
        "loc_year": 5
    },
    SOURCE_VETAPRO_ID: {
        "name": "vetapro",
        "nb_of_columns": 10,
        "loc_clinic_code": 0,
        "loc_clinic_name": 0,
        "loc_product_code": 3,
        "loc_product_name": 5,
        "loc_product_supplier": 4,
        "loc_product_gtin": None,
        "loc_product_vat": None,
        "loc_paid_quantity": 6,
        "loc_free_quantity": 7,
        "loc_amount": 9,
        "loc_month": 2,
        "loc_year": 1
    },
    SOURCE_VETYSPHARMA_ID: {
        "name": "vetyspharma",
        "nb_of_columns": 10,
        "loc_clinic_code": 0,
        "loc_clinic_name": 0,
        "loc_product_code": 1,
        "loc_product_name": 3,
        "loc_product_supplier": 7,
        "loc_product_gtin": 4,
        "loc_product_vat": None,
        "loc_paid_quantity": 6,
        "loc_free_quantity": None,
        "loc_amount": 9,
        "loc_month": 3,
        "loc_year": 2
    },
    SOURCE_HIPPOCAMPE_ID: {
        "name": "hippocampe",
        "nb_of_columns": 14,
        "loc_clinic_code": 0,
        "loc_clinic_name": 1,
        "loc_product_code": 2,
        "loc_product_name": 5,
        "loc_product_supplier": 6,
        "loc_product_gtin": 4,
        "loc_product_vat": None,
        "loc_paid_quantity": 13,
        "loc_free_quantity": None,
        "loc_amount": 12,
        "loc_month": 10,
        "loc_year": 11
    },
    SOURCE_AGRIPHARM_ID: {
        "name": "agripharm",
        "nb_of_columns": 16,
        "loc_clinic_code": 14,
        "loc_clinic_name": 15,
        "loc_product_code": 2,
        "loc_product_name": 3,
        "loc_product_supplier": 4,
        "loc_product_gtin": 5,
        "loc_product_vat": None,
        "loc_paid_quantity": 10,
        "loc_free_quantity": 11,
        "loc_amount": 12,
        "loc_month": 8,
        "loc_year": 7
    },
    SOURCE_ELVETIS_ID: {
        "name": "elvetis",
        "nb_of_columns": 10,
        "loc_clinic_code": 1,
        "loc_clinic_name": 0,
        "loc_product_code": 5,
        "loc_product_name": 4,
        "loc_product_supplier": 3,
        "loc_product_gtin": 37,
        "loc_product_vat": None,
        "loc_paid_quantity": 7,
        "loc_free_quantity": None,
        "loc_amount": 20,
        "loc_month": None,
        "loc_year": None
    },
    SOURCE_LONGIMPEX_ID: {
        "name": "longimpex",
        "nb_of_columns": 10,
        "loc_clinic_code": None,
        "loc_clinic_name": None,
        "loc_product_code": 2,
        "loc_product_name": 1,
        "loc_product_supplier": 0,
        "loc_product_gtin": 3,
        "loc_product_vat": None,
        "loc_paid_quantity": 4,
        "loc_free_quantity": None,
        "loc_amount": 5,
        "loc_month": None,
        "loc_year": None
    },
    SOURCE_DIRECT_ID: {
        "name": "direct"
    },
    SOURCE_CEDIVET_ID: {
        "name": "cedivet",
        "nb_of_columns": 15,
        "loc_clinic_code": 0,
        "loc_clinic_name": 1,
        "loc_product_code": 4,
        "loc_product_name": 5,
        "loc_product_supplier": 7,
        "loc_product_gtin": None,
        "loc_product_vat": None,
        "loc_paid_quantity": 8,
        "loc_free_quantity": None,
        "loc_amount": 10,
        "loc_month": None,
        "loc_year": None
    },
    SOURCE_COVETRUS_ID: {
        "name": "covetrus",
        "nb_of_columns": 19,
        "loc_clinic_code": 3,
        "loc_clinic_name": 4,
        "loc_product_code": 5,
        "loc_product_name": 6,
        "loc_product_supplier": 8,
        "loc_product_gtin": None,
        "loc_product_vat": 10,
        "loc_paid_quantity": 12,
        "loc_free_quantity": None,
        "loc_amount": 13,
        "loc_month": 1,
        "loc_year": 0
    }
}

sources_direct = {
    SUPPLIER_BIOVE_ID: {
        "name": "biove",
        "nb_of_columns": 42,
        "loc_clinic_code": 35,
        "loc_clinic_name": 4,
        "loc_product_code": 34,
        "loc_product_name": 7,
        "loc_product_supplier": 27,
        "loc_product_gtin": 11,
        "loc_product_vat": None,
        "loc_paid_quantity": 8,
        "loc_free_quantity": None,
        "loc_amount": 9,
        "loc_month": 1,
        "loc_year": 2
    },
    SUPPLIER_AUDEVARD_ID: {
        "name": "audevard"
    },
    SUPPLIER_AXIENCE_ID: {
        "name": "axience"
    },
    SUPPLIER_BAYER_ID: {
        "name": "bayer",
        "nb_of_columns": 16,
        "loc_clinic_code": 2,
        "loc_clinic_name": 3,
        "loc_product_code": 4,
        "loc_product_name": 5,
        "loc_product_supplier": None,
        "loc_product_gtin": None,
        "loc_product_vat": 11,
        "loc_paid_quantity": 9,
        "loc_free_quantity": None,
        "loc_amount": 10,
        "loc_month": 1,
        "loc_year": 0
    },
    SUPPLIER_BIMEDA_ID: {
        "name": "bimeda"
    },
    SUPPLIER_BOEHRINGER_ID: {
        "name": "boehringer",
        "nb_of_columns": 16,
        "loc_clinic_code": 2,
        "loc_clinic_name": 3,
        "loc_product_code": 4,
        "loc_product_name": 5,
        "loc_product_supplier": None,
        "loc_product_gtin": None,
        "loc_product_vat": 11,
        "loc_paid_quantity": 9,
        "loc_free_quantity": None,
        "loc_amount": 10,
        "loc_month": 1,
        "loc_year": 0
    },
    SUPPLIER_BOIRON_ID: {
        "name": "boiron"
    },
    SUPPLIER_CEVA_ID: {
        "name": "ceva",
        "nb_of_columns": 16,
        "loc_clinic_code": 2,
        "loc_clinic_name": 3,
        "loc_product_code": 4,
        "loc_product_name": 5,
        "loc_product_supplier": None,
        "loc_product_gtin": None,
        "loc_product_vat": 11,
        "loc_paid_quantity": 9,
        "loc_free_quantity": None,
        "loc_amount": 10,
        "loc_month": 1,
        "loc_year": 0
    },
    SUPPLIER_DOPHARMA_COOPHAVET_ID: {
        "name": "dopharma-coophavet"
    },
    SUPPLIER_DECHRA_ID: {
        "name": "dechra"
    },
    SUPPLIER_ELANCO_ID: {
        "name": "elanco",
        "nb_of_columns": 17,
        "loc_clinic_code": 2,
        "loc_clinic_name": 3,
        "loc_product_code": 4,
        "loc_product_name": 5,
        "loc_product_supplier": None,
        "loc_product_gtin": None,
        "loc_product_vat": 11,
        "loc_paid_quantity": 9,
        "loc_free_quantity": None,
        "loc_amount": 10,
        "loc_month": 1,
        "loc_year": 0
    },
    SUPPLIER_HILLS_ID: {
        "name": "hills"
    },
    SUPPLIER_HIPRA_ID: {
        "name": "hipra"
    },
    SUPPLIER_MERIAL_ID: {
        "name": "merial"
    },
    SUPPLIER_MP_LABO_ID: {
        "name": "mp-labo"
    },
    SUPPLIER_MSD_ID: {
        "name": "msd"
    },
    SUPPLIER_NESTLE_PURINA_ID: {
        "name": "nestle-purina"
    },
    SUPPLIER_OSALIA_ID: {
        "name": "osalia"
    },
    SUPPLIER_HUVEPHARMA_QALIAN_ID: {
        "name": "huvepharma-qalian"
    },
    SUPPLIER_ROYAL_CANIN_ID: {
        "name": "royal-canin",
        "nb_of_columns": 21,
        "loc_clinic_code": 2,
        "loc_clinic_name": 3,
        "loc_product_code": 4,
        "loc_product_name": 5,
        "loc_product_supplier": None,
        "loc_product_gtin": None,
        "loc_product_vat": 11,
        "loc_paid_quantity": 9,
        "loc_free_quantity": None,
        "loc_amount": 10,
        "loc_month": 1,
        "loc_year": 0
    },
    SUPPLIER_SAVETIS_ID: {
        "name": "savetis"
    },
    SUPPLIER_TVM_ID: {
        "name": "tvm"
    },
    SUPPLIER_VETOQUINOL_ID: {
        "name": "vetoquinol",
        "nb_of_columns": 16,
        "loc_clinic_code": 2,
        "loc_clinic_name": 3,
        "loc_product_code": 4,
        "loc_product_name": 5,
        "loc_product_supplier": None,
        "loc_product_gtin": None,
        "loc_product_vat": 11,
        "loc_paid_quantity": 9,
        "loc_free_quantity": None,
        "loc_amount": 10,
        "loc_month": 1,
        "loc_year": 0
    },
    SUPPLIER_VIRBAC_ID: {
        "name": "virbac"
    },
    SUPPLIER_ZOETIS_ID: {
        "name": "zoetis",
        "nb_of_columns": 19,
        "loc_clinic_code": 2,
        "loc_clinic_name": 3,
        "loc_product_code": 4,
        "loc_product_name": 5,
        "loc_product_supplier": None,
        "loc_product_gtin": None,
        "loc_product_vat": 11,
        "loc_paid_quantity": 9,
        "loc_free_quantity": None,
        "loc_amount": 10,
        "loc_month": 1,
        "loc_year": 0
    },
    SUPPLIER_POMMIER_ID: {
        "name": "pommier"
    },
    SUPPLIER_GENIA_ID: {
        "name": "genia"
    },
    SUPPLIER_OBIONE_ID: {
        "name": "obione"
    },
    SUPPLIER_VETALIS_ID: {
        "name": "vetalis"
    },
    SUPPLIER_OPTINAC_ID: {
        "name": "optinac"
    },
    SUPPLIER_LABO_IDT_ID: {
        "name": "labo-idt"
    },
    SUPPLIER_LABORATOIRE_LCV_ID: {
        "name": "laboratoire-lcv"
    },
    SUPPLIER_CEVA_BIOVAC_ID: {
        "name": "ceva-biovac"
    },
    SUPPLIER_FILAVIE_ID: {
        "name": "filavie"
    },
    SUPPLIER_CERTIVET_ID: {
        "name": "certivet"
    },
    SUPPLIER_GREENPEX_ID: {
        "name": "greenpex"
    },
    SUPPLIER_SYNHERGI_ID: {
        "name": "synhergi"
    },
    SUPPLIER_LDCA_ID: {
        "name": "ldca"
    },
    SUPPLIER_ALIVIRA_ID: {
        "name": "alivira"
    },
    SUPPLIER_PHYSAN_ID: {
        "name": "physan"
    },
    SUPPLIER_NOVACTIV_ID: {
        "name": "novactiv"
    },
    SUPPLIER_CODICO_TONIVET_ID: {
        "name": "tonivet"
    },
    SUPPLIER_SV_ALCYON_ID: {
        "name": "sv-alcyon"
    },
    SUPPLIER_TECHNOVET_ID: {
        "name": "technovet"
    },
    SUPPLIER_CALIBRA_ID: {
        "name": "calibra"
    },
    SUPPLIER_BIOTOPIS_ID: {
        "name": "biotopis"
    },
    SUPPLIER_WAMINE_ID: {
        "name": "wamine"
    },
    SUPPLIER_KRKA_ID: {
        "name": "krka"
    },
    SUPPLIER_MILOA_ID: {
        "name": "miloa"
    },
    SUPPLIER_ARCANATURA_ID: {
        "name": "arcanatura"
    },
    SUPPLIER_BD_ANIMALHEALTH_ID: {
        "name": "bd-animalhealth"
    },
    SUPPLIER_BBRAUN_ID: {
        "name": "bbraun"
    },
}