from neo4j import GraphDatabase
import pandas as pd
import config as config


class Neo4jConnection:

    def __init__(self, conn_url, user, pwd):
        self.conn_url = conn_url
        self.user = user
        self.pwd = pwd
        self.driver = None
        try:
            self.driver = GraphDatabase.driver(self.conn_url, auth=(self.user, self.pwd))
        except Exception as e:
            print("Failed to create the driver:", e)

    def close(self):
        if self.driver is not None:
            self.driver.close()

    def query(self, query, db=None):
        assert self.driver is not None, "Driver not initialized!"
        session = None
        response = None
        try:
            session = self.driver.session(database=db) if db is not None else self.driver.session()
            response = list(session.run(query))
        except Exception as e:
            print(f"Query {query} failed: {e}")
        finally:
            if session is not None:
                session.close()
        return response


# TODO data transformation should be performed on pandas columns rather than rows
def create_date_format(string):
    """
    Required date format for Neo4J is yyyy-mm-ddThh:mm:ss.xxx
    :param string:
    :return:
    """
    # TODO write a lot of general stuff to end up with correct format
    string = f"\'{string.replace(' ', 'T')}\'"
    return string


# TODO data transformation should be performed on pandas columns rather than rows
def create_string_format(string):
    """
    To deal with spaces in strings we need to enclose strings in 'example'
    :param string: text
    :return: string enclosed with ''
    """
    string = f"\'{string}\'"
    return string


# TODO data transformation should be performed on pandas columns rather than rows
def dealing_with_blanks(string):
    """
    In Neo4j the value null is used for actual non-available values. We should make sure all blank values are treated
    the same, by for instance replace all those values (both in string as integer columns to nan or whatsoever.
    :param string:
    :return:
    """
    string = f"\'{string}\'"
    return string


def create_node_company_row(row, connection, db_name):
    """
    function to create nodes company
    :param row: row of pandas dataframe
    :param connection: connection to Neo4J database
    :param db_name: database name
    :return: nothing, performs query in Neo4J
    """
    # Get relevant columns and transform them accordingly
    company_id = row['companyid']
    company_name = row['company_name']
    query_string = f"MERGE (c:Company {{company_id: {company_id}}}) " \
                   f"SET c.company_name = \'{company_name}\'" \
                   f"RETURN count(c);" \
                   f""

    connection.query(query_string, db=db_name)


def create_node_supplier_row(row, connection, db_name):
    """
    function to create nodes supplier
    :param row: row of pandas dataframe
    :param connection: connection to Neo4J database
    :param db_name: database name
    :return: nothing, performs query in Neo4J
    """
    # Get relevant columns and transform them accordingly
    supplier_id = row['supplierid']
    founded = create_date_format(row['founded'])
    country = create_string_format(row['country'])
    energy_label = create_string_format(row['energylabel'])
    supplier_name = create_string_format(row['supplier_name'])
    supplier_type = create_string_format(row['supplier_type'])

    # Create query
    query_string = f"MERGE (s:Supplier {{supplier_id: {supplier_id}}}) " \
                   f"SET " \
                   f"s.country = CASE WHEN {country} = 'nan' THEN null ELSE {country} END, " \
                   f"s.energy_label = CASE WHEN {energy_label} = 'nan' THEN null ELSE {energy_label} END, " \
                   f"s.supplier_name = CASE WHEN {supplier_name} = 'nan' THEN null ELSE {supplier_name} END, " \
                   f"s.supplier_type = CASE WHEN {supplier_type} = 'nan' THEN null ELSE {supplier_type} END, " \
                   f"s.founded = datetime({founded}) " \
                   f"RETURN count(s);"

    connection.query(query_string, db=db_name)


def create_rel_com_suppl_row(row, connection, db_name):
    """
    function to create relation SUPPLIES_TO between company and supplier
    :param row: row of pandas dataframe
    :param connection: connection to Neo4J database
    :param db_name: database name
    :return: nothing, performs query in Neo4J
    """
    # Get relevant columns and transform them accordingly
    company_id = row['companyid']
    supplier_id = row['supplierid']
    transport_via = create_string_format(row['transport_via'])
    supply_amount = row['supply_amount']

    # Create query
    query_string = f"MATCH (c:Company {{company_id: {company_id}}}) " \
                   f"MATCH (s:Supplier {{supplier_id: {supplier_id}}}) " \
                   f"MERGE (s)-[rel:SUPPLIES_TO]->(c) " \
                   f"SET " \
                   f"rel.transport_via = CASE WHEN {transport_via} = 'nan' THEN null ELSE {transport_via} END, " \
                   f"rel.quantity = CASE WHEN {supply_amount} = 'nan' THEN null ELSE {supply_amount} END " \
                   f"RETURN count(rel);"

    connection.query(query_string, db=db_name)


def create_rel_suppl_suppl_row(row, connection, db_name):
    """
    function to create relation SUPPLIES_TO between supplier and supplier
    :param row: row of pandas dataframe
    :param connection: connection to Neo4J database
    :param db_name: database name
    :return: nothing, performs query in Neo4J
    """
    # Get relevant columns and transform them accordingly
    from_id = row['supplierid_from']
    to_id = row['supplierid_to']
    quantity = row['quantity']

    # Create query
    query_string = f"MATCH (from:Supplier {{supplier_id: {from_id}}}) " \
                   f"MATCH (to:Supplier {{supplier_id: {to_id}}}) " \
                   f"MERGE (from)-[rel:SUPPLIES_TO]->(to) " \
                   f"SET " \
                   f"rel.quantity = {quantity} " \
                   f"RETURN count(rel);"

    connection.query(query_string, db=db_name)


if __name__ == '__main__':

    # Get the desired nodes and relationships in a pandas dataframe
    companies = pd.read_csv('./import/companies.csv', sep=';')
    suppliers = pd.read_csv('./import/suppliers.csv', sep=';')
    rel_comp_suppl = pd.read_csv('./import/company-supplier.csv', sep=';')
    rel_suppl_suppl = pd.read_csv('./import/supplier-supplier.csv', sep=';')

    # Create an instance of the connection
    conn = Neo4jConnection(conn_url=config.get_db_url(), user=config.get_db_user(), pwd=config.get_db_password())

    # Create the database (if it exists, the data is cleared)
    database_name = config.get_db_name()
    conn.query(f"CREATE OR REPLACE DATABASE {database_name}")

    # Add nodes based on the dataframe
    companies.apply(lambda row: create_node_company_row(row, connection=conn, db_name=database_name), axis=1)
    suppliers.apply(lambda row: create_node_supplier_row(row, connection=conn, db_name=database_name), axis=1)

    # Add relationships
    rel_comp_suppl.apply(lambda row: create_rel_com_suppl_row(row, connection=conn, db_name=database_name), axis=1)
    rel_suppl_suppl.apply(lambda row: create_rel_suppl_suppl_row(row, connection=conn, db_name=database_name), axis=1)

    # Get results from the graph-database
    query_all_nodes = '''
    MATCH (a)-[rel:SUPPLIES_TO]->(b)
    RETURN a, rel, b
    '''

    test = conn.query(query_all_nodes, db=database_name)
    print(test)

    query_subgraph = '''
    MATCH (company)<-[*]-(supplier)<-[*0..10]-(children)
    WHERE company.company_name = 'company_A'
    RETURN company, supplier, children
    '''

    test_subgraph = conn.query(query_subgraph, db=database_name)
    print(test_subgraph)
