# Licensed under the Apache License Version 2.0: http://www.apache.org/licenses/LICENSE-2.0.txt

from cassandra.cluster import Cluster
from py2neo import neo4j
import redis

NeoURL = "http://localhost:7474/db/data/"

cache = redis.StrictRedis()

neoDb = neo4j.GraphDatabaseService(NeoURL)

cassKeySpace = 'socialminer'
