'''
Scripts are not used, but are for showcase of the 'original' Cypher query
'''


def create_node_company(connection, db_name):
    query_string = '''
        USING PERIODIC COMMIT 500

        LOAD CSV WITH HEADERS FROM 'file:///companies.csv' AS row
        WITH toInteger(row.companyid) AS companyid, row.company_name AS company_name

        MERGE (c:Company {companyid: companyid})
            SET c.company_name = company_name

        RETURN count(c);
        '''

    connection.query(query_string, db=db_name)


def create_node_supplier(connection, db_name):
    query_string = '''
        USING PERIODIC COMMIT 500

        LOAD CSV WITH HEADERS FROM 'file:///suppliers.csv' AS row
        WITH toInteger(row.supplierid) AS supplierid, row.supplier_name AS supplier_name, 
                row.supplier_type AS supplier_type, datetime(replace(row.founded,' ','T')) AS founded, 
                row.country AS country, row.energylabel as energylabel

        MERGE (o:Supplier {supplierid: supplierid})
            SET o.founded = founded, o.country = country, o.energylabel = energylabel, 
                o.supplier_name = supplier_name, o.supplier_type = supplier_type

        RETURN count(o); 
        '''

    connection.query(query_string, db=db_name)


def create_relation_com_suppl(connection, db_name):
    query_string = '''
        USING PERIODIC COMMIT 500

        LOAD CSV WITH HEADERS FROM 'file:///company-supplier.csv' AS row
        WITH toInteger(row.supplierid) AS supplierid, toInteger(row.companyid) AS companyid, 
        toInteger(row.supply_amount) AS supply_amount, row.transport_via AS transport_via

        MATCH (c:Company {companyid: companyid})
        MATCH (s:Supplier {supplierid: supplierid})
        MERGE (s)-[rel:SUPPLIES_TO]->(c)
            SET rel.transport_via=transport_via, rel.supply_amount=supply_amount
        RETURN count(rel); 
        '''

    connection.query(query_string, db=db_name)


def create_relation_suppl_suppl(connection, db_name):
    query_string = '''
        USING PERIODIC COMMIT 500

        LOAD CSV WITH HEADERS FROM 'file:///supplier-supplier.csv' AS row
        WITH toInteger(row.supplierid_from) AS supplierid_from, toInteger(row.supplierid_to) AS supplierid_to, 
        toInteger(row.quantity) AS quantity

        MATCH (s_from:Supplier {supplierid: supplierid_from})
        MATCH (s_to:Supplier {supplierid: supplierid_to})
        MERGE (s_from)-[rel:SUPPLIES_TO]->(s_to)
            SET rel.quantity=quantity

        RETURN count(rel);
        '''

    connection.query(query_string, db=db_name)
