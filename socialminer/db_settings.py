# Licensed under the Apache License Version 2.0: http://www.apache.org/licenses/LICENSE-2.0.txt
from cassandra.cluster import Cluster
from py2neo import neo4j
import redis

cache = redis.StrictRedis()

NeoURL = "http://localhost:7474/db/data/"
neoDb = neo4j.GraphDatabaseService(NeoURL)

cassKeySpace = 'socialminer'

solrURL = 'http://localhost:8983/solr/'
