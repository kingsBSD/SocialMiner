from __future__ import absolute_import
# Licensed under the Apache License Version 2.0: http://www.apache.org/licenses/LICENSE-2.0.txt
""" Adjacency matrices for various queries relating to Twitter. """
import re

from py2neo import cypher, neo4j, node, rel

from socialminer.db_settings import *

def twitterFofQuery(user):
    fofQuery = """MATCH (a:twitter_user {screen_name:'SCREEN_NAME'})-[:FOLLOWS]->(b:twitter_user) WITH b
    MATCH (c:twitter_user)-[:FOLLOWS]->(b:twitter_user) WITH DISTINCT c
    MATCH (c)-[:FOLLOWS]->(d:twitter_user) RETURN DISTINCT c.screen_name,COLLECT(d.screen_name)"""
    return re.sub(r'SCREEN_NAME',user,fofQuery)   

def twitterTransFofQuery(user):
    fofQuery = """MATCH (a:twitter_user {screen_name:'SCREEN_NAME'})-[:FOLLOWS]->(b:twitter_user)-[:FOLLOWS]->(a) WITH b
    MATCH (c:twitter_user)-[:FOLLOWS]->(b:twitter_user)-[:FOLLOWS]->(c)
    RETURN DISTINCT b.screen_name,COLLECT(c.screen_name)"""
    return re.sub(r'SCREEN_NAME',user,fofQuery)

def twitterMatrix(query):
    """Run a Cypher query that returns pairs of Twitter screen_names lists of others to which they are linked."""
    data = neo4j.CypherQuery(neoDb,query).execute().data
    screenNames = [ i[0] for i in data if i[0] ]
    nameSet = set(screenNames)
    
    def getRow(row): 
        rowNames = set(data[row][1]).intersection(nameSet)
        return [ float(1 & (i in rowNames)) for i in screenNames ]

    return screenNames, [ getRow(i) for i in range(len(screenNames)) ]
    



