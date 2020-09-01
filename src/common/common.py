from sqlalchemy.sql import text


def get_id_of_country(connection, country_name):
    query = """
                    select distinct esf.srcf_country_id 
                    from ed_source_format esf 
                    where esf.srcf_country_name = :countryName
                        """

    result = connection.execute(text(query), {'countryName': country_name})
    country = result.fetchone()

    if country is None:
        raise KeyError(f"Country {country} not found !")
    else:
        return country["srcf_country_id"]


def get_name_of_country(connection, country_id):
    query = """
                    select distinct esf.srcf_country_name 
                    from ed_source_format esf 
                    where esf.srcf_country_id = :countryId
                        """

    result = connection.execute(text(query), {'countryId': country_id})
    country = result.fetchone()

    if country is None:
        raise KeyError(f"Country {country} not found !")
    else:
        return country["srcf_country_id"]


def get_name_of_source(connection, country_id, country_name, source_id, supplier_id):
    params = {'sourceId': source_id}
    query = """
                    select distinct esf.srcf_source_name, esf.srcf_supplier_name 
                    from ed_source_format esf 
                    where esf.srcf_source_id = :sourceId
                        """

    if country_id is not None:
        params['countryId'] = country_id
        query = query + """and esf.srcf_country_id = :countryId """

    if country_name is not None:
        params['countryName'] = country_name
        query = query + """and esf.srcf_country_name = :countryName """

    if supplier_id is not None:
        params['supplierId'] = supplier_id
        query = query + """and esf.srcf_supplier_id = :supplierId """

    result = connection.execute(text(query), params)
    source = result.fetchone()

    if source is None:
        raise KeyError(f"Source {source_id} not found for country !")
    else:
        if supplier_id is not None:
            return source["srcf_supplier_name"]
        else:
            return source["srcf_source_name"]


def get_id_of_source(connection, country_id, country_name, source_name):
    params = {'sourceName': source_name}

    query = """
                    select distinct esf.srcf_source_id, esf.srcf_supplier_id 
                    from ed_source_format esf 
                    where (esf.srcf_source_name = :sourceName or esf.srcf_supplier_name = :sourceName)
                        """

    if country_id is not None:
        params['countryId'] = country_id
        query = query + """and esf.srcf_country_id = :countryId """

    if country_name is not None:
        params['countryName'] = country_name
        query = query + """and esf.srcf_country_name = :countryName """

    result = connection.execute(text(query), params)
    source = result.fetchone()

    if source is None:
        raise KeyError(f"Source {source_name} not found for country !")
    else:
        return source["srcf_source_id"], source["srcf_supplier_id"]
