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
          MATCH (user:User {id: line.user_id}, (paper:Paper {id: line.paper_id})) \
          MERGE (user)-[:takes {id: line.id, paper_id: line.paper_id, user_id: line.user_id, score: line.score, accumulate_score: line.accumulate_score}]-(paper) \
          ")

def load_questions(tx):
    tx.run("LOAD CSV WITH HEADERS FROM 'file:///questions.csv' AS line \
          MATCH (p:Paper {id: line.paper_id}) \
          MERGE (q:Question {id: line.id, content: line.content, score: score}) \
          MERGE (q)-[:compose]->(p) \
          MERGE (p)-[:compose_of]->(q) \
          ")

def load_users_questions(tx):
    tx.run("LOAD CSV WITH HEADERS FROM 'file:///users_questions.csv' AS line \
        MATCH (user:User {id: line.user_id}, (q:Question {id: line.question_id})) \
        MERGE (user)-[:answers {id: line.writing, score: line.score}]-(q) \
        ")
    pass


def load_schools(tx):
    tx.run("LOAD CSV WITH HEADERS FROM 'file:///schools.csv' AS line \
          MERGE (c:School {id: line.id, name: line.name, region: line.region}) \
          ")


def load_school_users(tx):
    tx.run("LOAD CSV WITH HEADERS FROM 'file:///school_users.csv' AS line \
            MATCH (user:User {id:line.user_id}), (school:School {id:line.school_id}) \
            MERGE (user)-[:study_at {role: line.role, stage: line.stage, status: line.status}]->(school) \
           ")


#
def export_csv(table_name, standard=True):
    if standard == False:
        locals()["export_{0}_csv".format(table_name)]
    else: 
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

def export_users_questions_csv():
    logger.info("users_questions_csv")
    table_name = "users_questions"
    query = """
        with latest as (select id, max(sync_id) as sync_id from users_questions group by id), 
        latest_answers as (select id, max(sync_id) as sync_id from answers group by id)
        select users_questions.*, answers.writing from users_questions
        inner join latest on latest.sync_id = users_questions.sync_id
        inner join answers on users_questions.answer_id = answers.id
        inner join latest_answers on latest_answers.sync_id = answers.sync_id
    """.format(table_name)
    outputquery = "COPY ({0}) TO STDOUT WITH CSV HEADER".format(query)
    logger.info(outputquery)
    with analytical_db.conn.cursor() as cur:
        with open('import/{0}.csv'.format(table_name), 'w') as f:
            cur.copy_expert(outputquery, f)


def rebuild():
    pass


if __name__ == '__main__':
    dotenv.load_dotenv()

    # locals()["load_answers"](None)

    # Target: Load data from ana database, export to csv
    analytical_db = DB("ANALYTICAL_DB")

    # # 1. load from ana, export to csv
    # #  "papers", "questions", "users_papers", "users_questions",
    tables = [
        # ["users", True], 
        # ["schools", True], 
        # ["school_users", True],
        ["papers", True], 
        ["questions", True],
        ["users_questions", False],        
    ]
    for t, standard in tables:
        export_csv(t, standard)

    # # 2. transform to fit graph
    # 3. load from csv, merge to graph
    uri = 'bolt://neo4j.m2mda.com'
    driver = GraphDatabase.driver(uri, auth=("neo4j", os.getenv("NEO4J_PASS")))

    with driver.session() as session:
        for t in tables:
            logger.info("loading in {0}".format(t))
            session.write_transaction(locals()["load_{0}".format(t)])

    driver.close()
