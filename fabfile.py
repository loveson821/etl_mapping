from __future__ import with_statement
from fabric.api import env, local, settings, abort, run, cd, sudo
from fabric.contrib.console import confirm
import dotenv
import os

dotenv.load_dotenv()

# Hosts
env.user = os.getenv("NEO4J_VM_USER")
env.hosts = os.getenv("NEO4J_VM_HOST")
env.password = os.getenv("NEO4J_VM_PASS")


def stage():
    env.user = os.getenv("NEO4J_VM_USER")
    env.hosts = [os.getenv("NEO4J_VM_HOST")]
    global code_dir
    code_dir = "/home/{0}/sda/neo4j/etl_mapping".format(
        env.user)  # no trailing slash


def deploy():
    print(code_dir)
    run("sudo chmod 777 {0}/import -R".format(code_dir))
    run("git checkout .")
    run("git pull origin main")
    run("pip install -r requirements")
    run("python migrate_neo4j.py")