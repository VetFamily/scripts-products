from sqlalchemy.sql import text


def get_id_of_country(connection, country_name):
    query = """
                    select distinct id
                    from country
                    where code = :countryName
                        """

    result = connection.execute(text(query), {'countryName': country_name})
    country = result.fetchone()

    if country is None:
        raise KeyError(f"Country {country} not found !")
    else:
        return country["id"]


def get_name_of_country(connection, country_id):
    query = """
                    select distinct code 
                    from country
                    where id = :countryId
                        """

    result = connection.execute(text(query), {'countryId': country_id})
    country = result.fetchone()

    if country is None:
        raise KeyError(f"Country {country} not found !")
    else:
        return country["code"]


def get_name_of_source(connection, country_id, country_name, source_id, supplier_id):
    params = {'sourceId': source_id}
    query = """
                    select distinct ce.code as source_name, l.code as supplier_name 
                    from ed_source_format sf 
                    join country c on c.id = sf.srcf_country_id 
                    join centrales ce on ce.id = sf.srcf_source_id 
                    left join laboratoires l on l.id = sf.srcf_supplier_id 
                    where sf.srcf_source_id = :sourceId
                        """

    if country_id is not None:
        params['countryId'] = country_id
        query = query + """and sf.srcf_country_id = :countryId """

    if country_name is not None:
        params['countryName'] = country_name
        query = query + """and c.code = :countryName """

    if supplier_id is not None:
        params['supplierId'] = supplier_id
        query = query + """and sf.srcf_supplier_id = :supplierId """

    result = connection.execute(text(query), params)
    source = result.fetchone()

    if source is None:
        raise KeyError(f"Source {source_id} not found for country !")
    else:
        if supplier_id is not None:
            return source["supplier_name"]
        else:
            return source["source_name"]


def get_id_of_source(connection, country_id, country_name, source_name):
    params = {'sourceName': source_name}

    query = """
                    select distinct sf.srcf_source_id, sf.srcf_supplier_id 
                    from ed_source_format sf 
                    join country c on c.id = sf.srcf_country_id 
                    join centrales ce on ce.id = sf.srcf_source_id 
                    left join laboratoires l on l.id = sf.srcf_supplier_id 
                    where (ce.code = :sourceName or l.code = :sourceName)
                        """

    if country_id is not None:
        params['countryId'] = country_id
        query = query + """and esf.srcf_country_id = :countryId """

    if country_name is not None:
        params['countryName'] = country_name
        query = query + """and c.code = :countryName """

    result = connection.execute(text(query), params)
    source = result.fetchone()

    if source is None:
        raise KeyError(f"Source {source_name} not found for country !")
    else:
        return source["srcf_source_id"], source["srcf_supplier_id"]
