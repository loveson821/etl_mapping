import os
from datetime import datetime
from db import DB
import dotenv
from neo4j import GraphDatabase
from pdb import set_trace
from loguru import logger
# import logging

# neo4j_log = logging.getLogger("neo4j.bolt")
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
          WITH line \
          WHERE NOT line.description is null \
          MERGE (p:Paper {id: line.id, name: coalesce(line.description, '')}) \
          ")


def load_users_papers(tx):
    tx.run("LOAD CSV WITH HEADERS FROM 'file:///users_papers.csv' AS line \
          MATCH (user:User {id: line.user_id}, (paper:Paper {id: line.paper_id})) \
          MERGE (user)-[:takes {id: line.id, paper_id: line.paper_id, user_id: line.user_id, score: line.score, accumulate_score: line.accumulate_score}]-(paper) \
          ")


def load_questions(tx):
    tx.run("LOAD CSV WITH HEADERS FROM 'file:///questions.csv' AS line \
          MATCH (p:Paper {id: line.paper_id}) \
          WITH line \
          WHERE NOT line.score is null \
          MERGE (q:Question {id: line.id, content: coalesce(line.content, ''), score: line.score}) \
          MERGE (q)-[:compose]->(p) \
          MERGE (p)-[:compose_of]->(q) \
          ")


def load_users_questions(tx):
    tx.run("LOAD CSV WITH HEADERS FROM 'file:///users_questions.csv' AS line \
        MATCH (user:User {id: line.user_id}), (q:Question {id: line.question_id}) \
        MERGE (user)-[:answers {id: line.id, writing: coalesce(line.writing, ''), score: coalesce(line.score, 0)}]->(q) \
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


def load_tags(tx):
    tx.run("LOAD CSV WITH HEADERS FROM 'file:///tags.csv' AS line \
          MERGE (c:Tag {id: line.id, name: line.name}) \
          ")


def load_taggings(tx):
    tx.run("LOAD CSV WITH HEADERS FROM 'file:///taggings.csv' AS line \
            WITH line.taggable_type AS label, line.tag_id as tagId, line.taggable_id as taggable_id \
            CALL apoc.cypher.run( 'MATCH (n:' + label + ' {id: \"'+ taggable_id + '\"}), (t:Tag {id: \"' + tagId + '\"}) return n,t' , {}) \
            YIELD value as data \
            unwind data.n as n \
            unwind data.t as t \
            MERGE (n)-[:has_tag]->(t) \
        ")
    pass


def export_taggings_csv():
    logger.info("exporting taggings csv")
    table_name = "taggings"
    query = """
        with latest as (select id, max(sync_id) as sync_id from taggings group by id), 
        latest_tags as (select id, max(sync_id) as sync_id from tags group by id)
        select taggings.*, tags.name from taggings
        inner join latest on latest.sync_id = taggings.sync_id
        inner join tags on tags.id = taggings.tag_id
        inner join latest_tags on latest_tags.sync_id = tags.sync_id
    """.format(table_name)
    write_csv_file(table_name, query)


def export_users_questions_csv():
    logger.info("exporting users_questions csv")
    table_name = "users_questions"
    query = """
        with latest as (select id, max(sync_id) as sync_id from users_questions group by id), 
        latest_answers as (select id, max(sync_id) as sync_id from answers group by id)
        select users_questions.*, answers.writing from users_questions
        inner join latest on latest.sync_id = users_questions.sync_id
        inner join answers on users_questions.answer_id = answers.id
        inner join latest_answers on latest_answers.sync_id = answers.sync_id
    """.format(table_name)
    write_csv_file(table_name, query)


def write_csv_file(name, sql):
    logger.info(sql)
    outputquery = "COPY ({0}) TO STDOUT WITH CSV HEADER".format(sql)
    with analytical_db.conn.cursor() as cur:
        with open('import/{0}.csv'.format(name), 'w') as f:
            cur.copy_expert(outputquery, f)


def export_csv(t):
    table_name = t['table_name']
    if t['standard_query_sql'] == False:
        return globals()["export_{0}_csv".format(table_name)]()
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


def create_indexes(tx, t):
    if t['edge'] == False:
        logger.info("create_indexes {0}".format(t))
        tx.run(
            "CREATE INDEX {0}_{1}_index IF NOT EXISTS FOR (t:{0}) ON (t.{1})".
            format(t['table_name'], t['id']))
    pass


def rebuild():
    pass


def load_tiku_csv_indexes(tx):
    tx.run("CREATE INDEX IF NOT EXISTS FOR (c:TextBook) ON (c.name)")
    tx.run("CREATE INDEX IF NOT EXISTS FOR (c:Chapter) ON (c.name)")
    tx.run("CREATE INDEX IF NOT EXISTS FOR (c:SubChapter) ON (c.name)")
    tx.run("CREATE INDEX IF NOT EXISTS FOR (c:TextBook) ON (c.name)")
    tx.run("CREATE INDEX IF NOT EXISTS FOR (c:Concept) ON (c.name)")
    tx.run(
        "CREATE INDEX IF NOT EXISTS FOR (c:TQuestion) ON (c.id, c.question_type)"
    )


def load_tiku_csv(tx):
    tx.run("LOAD CSV WITH HEADERS FROM 'file:///tiku.csv' AS line \
          unwind line.textbook_version +  '-' + line.textbook as textbook_name \
          unwind line.textbook_version +  '-' + line.textbook + '-' + line.chapter as chapter_name \
          unwind line.textbook_version +  '-' + line.textbook + '-' + line.chapter + '-' + line.subchapter as subchapter_name \
          MERGE (b:TextBook {name: textbook_name}) \
          MERGE (c:Chapter {name: chapter_name})  \
          MERGE (s:SubChapter {name: subchapter_name}) \
          MERGE (k: Concept {name: line.knowledge_point}) \
          MERGE (q: TQuestion {id: line.id, content: line.problem, question_type: line.question_type, difficulty: line.difficulty}) \
          MERGE (b)-[:chapters]->(c) \
          MERGE (c)-[:subchapters]->(s) \
          MERGE (s)-[:concepts]->(k) \
          MERGE (k)-[:questions]->(q) \
        ")


def randon_assign_question_to_concept(tx):
    tx.run(" \
        WITH range(1,1) as conceptRange \
        MATCH (h:Concept) \
        WITH collect(h) as concepts, conceptRange \
        MATCH (p:Question) \
        WITH p, apoc.coll.randomItems(concepts, apoc.coll.randomItem(conceptRange)) as concepts \
        FOREACH (concept in concepts | CREATE (p)-[:has_concept]->(concept)) \
    ")


if __name__ == '__main__':
    dotenv.load_dotenv()

    # locals()["load_answers"](None)

    # Target: Load data from ana database, export to csv
    analytical_db = DB("ANALYTICAL_DB")
    uri = os.getenv("NEO4J_BOLT_CONNECTION")
    driver = GraphDatabase.driver(uri, auth=("neo4j", os.getenv("NEO4J_PASS")))

    # # 1. load from ana, export to csv
    tables = [{
        "table_name": "users",
        "node_label": "User",
        "edge": False,
        "standard_query_sql": True
    }, {
        "table_name": "schools",
        "node_label": "School",
        "edge": False,
        "standard_query_sql": True
    }, {
        "table_name": "school_users",
        "node_label": "SchoolUser",
        "edge": False,
        "standard_query_sql": True
    }, {
        "table_name": "papers",
        "node_label": "Paper",
        "edge": False,
        "standard_query_sql": True
    }, {
        "table_name": "questions",
        "node_label": "Question",
        "edge": False,
        "standard_query_sql": True
    }, {
        "table_name": "users_questions",
        "node_label": "answers",
        "edge": True,
        "standard_query_sql": False
    }, {
        "table_name": "taggings",
        "node_label": "has_tags",
        "edge": True,
        "standard_query_sql": False
    }]

    # tables = tables[-1:]
    # for t in tables:
    #     export_csv(t)

    # # 2. load from csv, merge to graph
    # with driver.session() as session:
    #     for t in tables:
    #         logger.info("loading in {0}".format(t['table_name']))
    #         session.write_transaction(locals()["load_{0}".format(
    #             t['table_name'])])
    #         logger.info("create index on {0}".format(t['table_name']))
    #         session.write_transaction(create_indexes, t)

    # # 3 Tiki Data
    # with driver.session() as session:
    #     session.write_transaction(load_tiku_csv_indexes)

    # with driver.session() as session:
    #     session.write_transaction(load_tiku_csv)

    # 4 Testing data use only
    with driver.session() as session:
        session.write_transaction(randon_assign_question_to_concept)

    driver.close()
