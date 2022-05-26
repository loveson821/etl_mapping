import os
from datetime import datetime
from db import DB
import dotenv
from neo4j import GraphDatabase
from pdb import set_trace
from loguru import logger

# def create_user(tx, user):
# 	tx.run("CREATE (n:User {name: '', title: 'Developer'})")


def connection_test():
    uri = "bolt://neo4j.m2mda.com"
    driver = GraphDatabase.driver(uri, auth=("neo4j", os.getenv("NEO4J_PASS")))

    def create_person(driver, name):
        with driver.session() as session:
            result = session.run(
                "CREATE (a:Person { name: $name }) RETURN id(a) AS node_id",
                name=name)
            record = result.single()
            return record["node_id"]

    def get_friends_of(tx, name):
        friends = []
        result = tx.run(
            "MATCH (a:Person)-[:KNOWS]->(f) "
            "WHERE a.name = $name "
            "RETURN f.name AS friend",
            name=name)
        for record in result:
            friends.append(record["friend"])
        return friends

    with driver.session() as session:
        friends = session.read_transaction(get_friends_of, "Alice")
        for friend in friends:
            print(friend)

    create_person(driver, "Ronald")
    driver.close()


def load_users(tx):
    tx.run("LOAD CSV WITH HEADERS FROM 'file:///users.csv' AS line \
            MERGE (c:User {id: line.id, name: coalesce(line.name, ''), email: line.email}) \
          ")


def load_papers(tx):
    tx.run("LOAD CSV WITH HEADERS FROM 'file:///papers.csv' AS line \
          MERGE (c:Paper {id: line.id, name: line.description}) \
          ")


def load_users_papers(tx):
    tx.run("LOAD CSV WITH HEADERS FROM 'file:///users_papers.csv' AS line \
          MERGE (c:Paper {id: line.id, paper_id: line.paper_id, user_id: line.user_id, score: line.score, accumulate_score: line.accumulate_score}) \
          ")


def load_users_questions(tx):
    pass


def load_answers(tx):
    logger.info("ok")
    pass


def load_schools(tx):
    tx.run("LOAD CSV WITH HEADERS FROM 'file:///schools.csv' AS line \
          MERGE (c:School {id: line.id, name: line.name, region: line.region}) \
          ")


def load_school_users(tx):
    tx.run("LOAD CSV WITH HEADERS FROM 'file:///school_users.csv' AS line \
            MATCH (user:User {id:line.user_id}), (school:School {id:line.school_id}) \
            MERGE (user)-[:study_at]->(school) \
           ")


#
def export_csv(table_name):
    query = """
        with latest as (select id, max(sync_id) as sync_id from {0} group by id)
        select * from {0}
        inner join latest on latest.sync_id = {0}.sync_id
    """.format(table_name)
    outputquery = "COPY ({0}) TO STDOUT WITH CSV HEADER".format(query)
    logger.info(outputquery)
    with analytical_db.conn.cursor() as cur:
        with open('import/{0}.csv'.format(table_name), 'w') as f:
            cur.copy_expert(outputquery, f)
    pass


if __name__ == '__main__':
    dotenv.load_dotenv()

    # locals()["load_answers"](None)

    # Target: Load data from ana database, export to csv
    analytical_db = DB("ANALYTICAL_DB")

    # # 1. load from ana, export to csv
    # #  "papers", "questions", "users_papers", "users_questions",
    tables = ["users", "schools", "school_users"]
    # for t in tables:
    #     export_csv(t)

    # # 2. transform to fit graph
    # 3. load from csv, merge to graph
    uri = 'bolt://neo4j.m2mda.com'
    driver = GraphDatabase.driver(uri, auth=("neo4j", os.getenv("NEO4J_PASS")))

    with driver.session() as session:
        for t in tables:
            logger.info("loading in {0}".format(t))
            session.write_transaction(locals()["load_{0}".format(t)])

    driver.close()
