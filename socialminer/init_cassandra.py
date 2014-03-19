from __future__ import absolute_import
# Licensed under the Apache License Version 2.0: http://www.apache.org/licenses/LICENSE-2.0.txt
import re

from cassandra.cluster import Cluster

from socialminer.db_settings import cassKeySpace
from socialminer.twitter_tools import cassTwitterUserFields, cassTweetFields

def cassTable(table,fields,keys):
    query = 'CREATE TABLE IF NOT EXISTS '+table+' (\n'
    for field in fields:
        fieldType = False
        if field == 'isotime':
            fieldType = 'timestamp'
        if field in [u'verified','protected',u'geo_enabled']:
            fieldType = 'boolean'
        if field in ['utc_offset']:
            fieldType = 'int'
        if re.match(r'.*_count$|^id$',field):
            fieldType = 'bigint'
        if re.match(r'.*_id$',field):
            fieldType = 'bigint'            
        if not fieldType:
            fieldType = 'text'     
        query += '\t'+' '.join([field,fi# Licensed under the Apache License Version 2.0: http://www.apache.org/licenses/LICENSE-2.0.txteldType+',\n'])
    query += 'PRIMARY KEY ('+','.join(keys)+'));'
    return query

def initAllTheTables(show=False):
    taggedTweetFields = ['text','user_id_str','tweet_id_str','isotime']
    twitterMentionsFields = ['target_id_str','user_id_str','tweet_id_str','isotime']
    twitterLinkFields = ['url','expanded_url','user_id_str','tweet_id_str','isotime']
    twitterReplyFields = ['id_str','user_id_str','reply_id_str','reply_user_id_str','isotime']
    
    allTheTables = [
            ('twitter_user_lookup',['id_str','screen_name'],['screen_name']),
            ('twitter_user',cassTwitterUserFields,['id_str','screen_name']),
            ('twitter_user_friends',['id_str','friend_id_str','friend_screen_name'],['id_str','friend_id_str']),
            ('twitter_user_followers',['id_str','follower_id_str','follower_screen_name'],['id_str','follower_id_str']),
            ('tweet',cassTweetFields,['id_str','user_id_str','isotime']),
            ('user_tweet',['user_id_str','isotime','id_str'],['user_id_str','isotime']),
            ('retweet_by',['rt_user_id_str','user_id_str','isotime','id_str','rt_id_str'],['rt_user_id_str','user_id_str','isotime']),
            ('retweet_of',['user_id_str','rt_user_id_str','isotime','id_str','rt_id_str'],['user_id_str','rt_user_id_str','isotime']),
            ('tagged_tweet',taggedTweetFields,['text','isotime']),
            ('user_tagged_tweet',taggedTweetFields,['user_id_str','text','isotime']),
            ('twitter_mentions_of',twitterMentionsFields,['target_id_str','user_id_str','isotime']),
            ('twitter_mentions_by',twitterMentionsFields,['user_id_str','target_id_str','isotime']),
            ('twitter_links_by',twitterLinkFields,['user_id_str','isotime','expanded_url']),
            ('twitter_links_of',twitterLinkFields,['expanded_url','isotime']),
            ('twitter_replies_by',twitterReplyFields,['reply_user_id_str','user_id_str','isotime']),
            ('twitter_replies_to',twitterReplyFields,['user_id_str','reply_user_id_str','isotime'])
        ]
    
    renderedTables = [ cassTable(*table) for table in allTheTables ]
 
    cassCluster = Cluster()

    cassSession = cassCluster.connect()
    cassSession.execute("CREATE KEYSPACE IF NOT EXISTS "+cassKeySpace+" WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor' : 1};")
    cassSession.set_keyspace(cassKeySpace) 
 
 
    if show:
        print '\n\n'.join(renderedTables)
    else:
        map(cassSession.execute,renderedTables)


        
        
        





