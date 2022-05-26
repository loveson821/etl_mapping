import os
from datetime import datetime
from db import DB
import dotenv
from neo4j import GraphDatabase
from pdb import set_trace

# def create_user(tx, user):
# 	tx.run("CREATE (n:User {name: '', title: 'Developer'})")

def connection_test():
  uri = "bolt://neo4j.m2mda.com"
  driver = GraphDatabase.driver(uri, auth=("neo4j", "m2mneo4j123"))

  def create_person(driver, name):
    with driver.session() as session:
      result = session.run("CREATE (a:Person { name: $name }) RETURN id(a) AS node_id", name=name)
      record = result.single()
      return record["node_id"]

  def get_friends_of(tx, name):
      friends = []
      result = tx.run("MATCH (a:Person)-[:KNOWS]->(f) "
                          "WHERE a.name = $name "
                          "RETURN f.name AS friend", name=name)
      for record in result:
          friends.append(record["friend"])
      return friends

  with driver.session() as session:
      friends = session.read_transaction(get_friends_of, "Alice")
      for friend in friends:
        print(friend)

  create_person(driver, "Ronald")
  driver.close()


def load_csv(tx):
  tx.run("LOAD CSV WITH HEADERS FROM 'file:///Users/ronaldng/etl_mapping/users.csv' AS line \
           MERGE (c:User {id: line.id, name: line.name, email: line.email}) \
           ")

if __name__ == '__main__':
  dotenv.load_dotenv()

  # Target: Load data from ana database, export to csv
  analytical_db = DB("ANALYTICAL_DB")

  # 1. load from ana
  query = "select * from users limit 10"
  outputquery = "COPY ({0}) TO STDOUT WITH CSV HEADER".format(query)
  print(outputquery)
  with analytical_db.conn.cursor() as cur:
      with open('users.csv', 'w') as f:
          cur.copy_expert(outputquery, f)

  # 2. transform to fit graph
  # 3. merge to graph
  uri = 'bolt://neo4j.m2mda.com'
  driver = GraphDatabase.driver(uri, auth=("neo4j", "m2mneo4j123"))

  with driver.session() as session:
    session.write_transaction(load_csv)
		# session.write_transaction(add_friend, "Arthur", "Guinevere")
		# session.write_transaction(add_friend, "Arthur", "Lancelot")
		# session.write_transaction(add_friend, "Arthur", "Merlin")
		# session.read_transaction(print_friends, "Arthur")

  driver.close()
