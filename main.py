from neo4j import GraphDatabase
import pandas as pd
import os


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


if __name__ == '__main__':

    file_path = os.path.abspath(os.getcwd())

    companies = pd.read_csv('./import/companies.csv', sep=';')
    suppliers = pd.read_csv('./import/suppliers.csv', sep=';')

    # Create an instance of the connection
    conn = Neo4jConnection(conn_url="bolt://localhost:7687", user="rabobunny", pwd="rabobunny")

    # Create a new database
    database_name = 'rabobank1'
    conn.query(f"CREATE OR REPLACE DATABASE {database_name}")

    # Create nodes and relationships from csvs
    create_node_company(connection=conn, db_name=database_name)
    create_node_supplier(connection=conn, db_name=database_name)
    create_relation_com_suppl(connection=conn, db_name=database_name)
    create_relation_suppl_suppl(connection=conn, db_name=database_name)

    # Get results from the graph-database
    query = '''
    MATCH (a)-[rel:SUPPLIES_TO]->(b)
    RETURN a, rel, b
    '''

    test = conn.query(query, db=database_name)
    print(test)

    query_subgraph = '''
    MATCH (company)<-[*]-(supplier)<-[*0..10]-(children)
    WHERE company.companyid = 1
    RETURN company, supplier, children
    '''

    test_subgraph = conn.query(query_subgraph, db=database_name)
    print(test_subgraph)







