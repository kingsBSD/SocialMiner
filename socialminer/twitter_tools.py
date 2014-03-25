from __future__ import absolute_import
# Licensed under the Apache License Version 2.0: http://www.apache.org/licenses/LICENSE-2.0.txt

__author__ = 'Giles Richard Greenway'

from datetime import datetime, timedelta
import json
import pprint
import re

from cassandra.cluster import Cluster
from py2neo import cypher, neo4j, node, rel
import redis
from twython import Twython, TwythonAuthError, TwythonRateLimitError, TwythonError

from socialminer.db_settings import *
from socialminer.twitter_settings import *
from socialminer.solr_tools import addSolrDocs

"""
Fields of interest from twitter users and tweets returned by Twython.
Extra fields provided by the various render methods are appended.
"""
twitterUserFields = [u'id', u'id_str', u'verified', u'profile_image_url_https', u'followers_count', u'listed_count',
u'utc_offset',u'statuses_count', u'description', u'friends_count', u'location', u'profile_image_url', u'geo_enabled',
u'screen_name', u'lang',u'favourites_count',u'name', u'url', u'created_at', u'time_zone', u'protected'] + [u'isotime',u'last_scraped',u'__temp_label__']

tweetFields = [u'text',u'in_reply_to_status_id',u'id',u'favorite_count',u'source',u'retweeted',
    u'in_reply_to_screen_name',u'id_str',u'retweet_count',u'in_reply_to_user_id',u'favorited',
    u'in_reply_to_user_id_str',u'possibly_sensitive',u'lang',u'created_at',u'in_reply_to_status_id_str'] + [u'isotime',u'last_scraped',u'__temp_label__']

cassTwitterUserFields = twitterUserFields[:-1]
cassTweetFields = tweetFields[:-1] + ['user_id_str']

userIndex = neoDb.get_or_create_index(neo4j.Node, 'twitter_user')
tweetIndex = neoDb.get_or_create_index(neo4j.Node, 'tweet')
retweetIndex = neoDb.get_or_create_index(neo4j.Node, 'retweet')
friendIndex = neoDb.get_or_create_index(neo4j.Relationship, 'friends')

#def buildNeoIndices():
#    indices = [{'label':'twitter_user','keys':['screen_name']},{'label':'tweet','keys':['id']}]
#    #existing = neoDb.get_indexes(neo4j.Node)
#    for index in indices:
#        for key in index['keys']:
#            try:		
#                neoDb.schema.create_index(index['label'],key)
#            except:
#                pass

class ratedTwitter(object):
    """Wrapper around the Twython class that tracks whether API calls are rate-limited."""
    def __can_we_do_that__(self,methodName):
        """Check whether a given API call is rate-limited, return the estimated time to wait in seconds.
    
        Positional arguments:
        methodName -- the name of the API call to test    
        """      
        keyval = cache.get(self.handle+methodName) # Have we recorded how many calls remain in the current window?
        if not keyval: # We've not made the call for these credentials. Assume all's well.
            return 0
        else:
            history = json.loads(keyval)
            if history['limit'] > 0: # Still good to go.
                return 0
            reset = datetime.strptime( history['reset'].split('.')[0], "%Y-%m-%dT%H:%M:%S" )
            rightNow = datetime.now()
            if reset > rightNow: # No calls left and the window reset is in the future...
                return (reset - rightNow).seconds + 30 # ...return the time to wait.
            return 0

    def __method_call__(self,methodName,args):
        """Make a Twitter API call via the underlying Twython object.
    
        Returns a tuple: (True,<API call return value>) | (False,<reason for failure>)
    
        Positional arguments:
        methodName -- the name of the API call to test
        args -- dictionary of keyword arguments
        """ 
        try: # Does Twython even know how to do that?
            method = getattr(self.twitter,methodName)
        except:
            print '*** NO SUCH TWITTER METHOD: '+methodName+' ***'
            return (False,'no_such_method')
        
        try:
            result = (True,method(**args)) # Call the method of the Twython object.
        except TwythonAuthError:
            print '*** TWITTER METHOD 401: '+methodName+' ***'
            result = (False,'forbidden')
        except TwythonRateLimitError:
            print '*** TWITTER METHOD LIMITED: '+methodName+' ***'
            result = (False,'limited')
        except TwythonError as e:
            if str(e.error_code) == '404':
                print '*** TWITTER METHOD 404: '+methodName+' ***'
                result = (False,'404')
            else:
                print '*** TWITTER METHOD FAILED: '+methodName+' ***'
                result = (False,'unknown')
            print args

        try: # Have we been told how many calls remain in the current window?
            xLimit = self.twitter.get_lastfunction_header('x-rate-limit-remaining')
            xReset = self.twitter.get_lastfunction_header('x-rate-limit-reset')
        except:
            xLimit = xReset = False
            
        if xLimit:
            limit = int(xLimit)        
        if xReset:
            reset = datetime.utcfromtimestamp(int(xReset)).isoformat()
        if xLimit and xReset: # Store the current number of remaining calls and time when the window resets.
            cache.set(self.handle+methodName,json.dumps({'limit':limit, 'reset':reset})) 

        return result

    def __init__(self,credentials=False,useLocal=False):
        
        if not credentials:
            if useLocal:
                self.twitter = Twython(CONSUMER_KEY,CONSUMER_SECRET,OAUTH_TOKEN,OAUTH_TOKEN_SECRET)
                self.handle = 'local_'
            else:
                self.twitter = Twython(CONSUMER_KEY,access_token=ACCESS_TOKEN)
                self.handle = 'app_'
                
"""
Attach the following API calls to the ratedTwitter class, so that <ratedTwitter>.<method>(**args) makes the call via <ratedTwitter.__method_call__
and <ratedTwitter>.<method>_limited() makes the appropriate call to __can_we_do_that__.
"""
for name in ['lookup_user','get_friends_list','get_followers_list','get_user_timeline']:
    def f(self,name=name,**args): # http://math.andrej.com/2009/04/09/pythons-lambda-is-broken/
        return self.__method_call__(name,args)
    setattr(ratedTwitter,name,f)
    
    def g(self,name=name):
        return self.__can_we_do_that__(name)
    setattr(ratedTwitter,name+'_limited',g)    
    
        
def getTwitterAPI(credentials=False):
    """Return a Twitter API object from oauth credentials, defaulting to those in db_settings."""
    if not credentials:
        return Twython(CONSUMER_KEY,access_token=ACCESS_TOKEN)

    
noSlash = q=re.compile(r'\\')

def cypherVal(val):
    """Escape quotes and slashes for use in Cypher queries."""    
    if isinstance(val, (int, long, bool)):
        return unicode(val)
    else:
        escval = re.sub(noSlash,r'\\\\',val) # Escape all the backslashes.
        if "'" in escval:
            return u"'"+unicode(re.sub("'","\\'",escval))+"'"
        else:
            return u"'"+unicode(escval)+"'"

# Return Twitter's time format as isoformat.
twitterTime = lambda d: datetime.strptime(re.sub('[+\-][0-9]{4}\s','',d),'%a %b %d %X %Y').isoformat()

#def twitterUser2Cypher(user):
#    return u'('+user['screen_name']+u':twitter_user { '+ u', '.join([ field+u":"+ cypherVal(user[field])
#        for field in  twitterUserFields if user.get(field,False) ]) + u' })'

def renderTwitterUser(user):
    """Return a serializable dictionary of relevant fields for a Twitter user."""
    twit = dict([ (field,user[field]) for field in  twitterUserFields if user.get(field,False) ]) # Strip out the stuff we don't want to store.
    if user.get('created_at',False): # Suplement Twitter's time field with something saner.
        twit['isotime'] = twitterTime(user['created_at'])    
    return twit

def setUserDefunct(user):
    try:
        userNode=neoDb.find('twitter_user', property_key='screen_name', property_value=user).next()
    except:
        return  
    userNode.update_properties({'defunct':'true'})

def pushUsers2Neo(renderedTwits):
    """Store  a list of rendered Twitter users in Neo4J. No relationships are formed."""
    for twit in renderedTwits:
        twitNode = userIndex.get_or_create('id_str', twit['id_str'],twit)
        try:
            twitNode.add_labels('twitter_user')
        except:
            pass
        
def quoteCassVal(v):
    """Convert a value into a suitable string for Cassandra, enclosing in quoutes and escaping as needed."""
    if  isinstance(v,(int,long,bool)):
        return str(v)    
    return "'"+unicode(re.sub("'","''",v))+"'"

def pushUsers2Cass(renderedTwits,cassSession=False):
    """Store a list of rendered Twitter users in Cassandra.
       
    Positional arguments:
    renderedTwits -- a list of Twitter users rendered by "renderTwitterUser"

    Keyword arguments:
    cassSession -- Cassandra session, created using the default db_settings if not supplied

    """ 
    if not cassSession:
        cassCluster = Cluster()
        cassSession = cassCluster.connect()
        cassSession.set_keyspace('socialminer')
    query = 'BEGIN BATCH'
    for twit in renderedTwits: # Produce corresponding lists of field-names and their values.
        twitKeys, twitValues = zip(*[ (key,twit[key]) for key in cassTwitterUserFields if twit.get(key,False) ])
        quotedValues = [ quoteCassVal(v) for v in twitValues ]
        lookUpValues = [ "'"+v+"'" for v in [twit['id_str'],twit['screen_name']] ] # Values for the denormalised twitter_user_lookup table.
        query += ' INSERT INTO twitter_user ('+', '.join(twitKeys)+') VALUES ('+ ', '.join(quotedValues)+');'
        query += ' INSERT INTO twitter_user_lookup (id_str,screen_name) VALUES ('+', '.join(lookUpValues)+');'
    query += ' APPLY BATCH'

    try:
        cassSession.execute(query)
    except:
        print query
#def tweet2Cypher(tweet):
#    return u'('+tweet['id_str']+u':tweet { '+ u', '.join([field+u":"+ cypherVal(tweet[field])
#       for field in tweetFields if tweet.get(field,False)]) + u' })'

def renderTweet(tweet):
    """Return a serializable dictionary of relevant fields for a tweet."""
    rendered = dict([ (field,tweet[field]) for field in  tweetFields if tweet.get(field,False) ])
    if tweet.get('created_at',False):
        rendered['isotime'] = twitterTime(tweet['created_at'])
    if tweet.get('coordinates',False):
        lng, lat = tweet['coordinates']['coordinates']
        rendered['longitude'] = lng
        rendered['latitude'] = lat
    if tweet.get('user',False):
        rendered['user'] = renderTwitterUser(tweet['user'])
    return rendered

def filterTweets(tweets):
    """Decompose a list of tweets returned by Twython into lists of rendered tweets, retweets, mentions, hastags, URLs and replies."""

    allTweets = []
    allRetweets = []

    allMentions = []
    allHashTags = []
    allURLs = []        
    allTweetReplies = []
    allUserReplies = []
    
    started = datetime.now()

    def pushEntities(ment,hsh,url,tw):
        """Create tuples containing a rendered tweet and each of its mentions, hastags and URLs."""
        for mention in ment:
            allMentions.append((tw,mention))
        for hashTag in hsh:
            allHashTags.append((tw,hashTag))            
        for URL in url:
            allURLs.append((tw,URL))
            
    for tweet in tweets:
        
        if tweet['retweeted']:
            
            status = tweet['retweeted_status']

            renderedTweet = renderTweet(status)
            renderedRetweet = renderTweet(tweet)
        
            allRetweets.append((renderedTweet,renderedRetweet))
            # Get entities/replies from the *original* tweet.
            mentions, hashTags, URLs = [ status['entities'][field] for field in ['user_mentions','hashtags','urls'] ]
            pushEntities(mentions,hashTags,URLs,renderedTweet)
            if status.get('in_reply_to_status_id_str',False):
                allTweetReplies.append((renderedTweet,{'id_str':status['in_reply_to_status_id_str'],'id':status['in_reply_to_status_id']}))
            if status.get('in_reply_to_user_id_str',False):
                allUserReplies.append((renderedTweet,{'id_str':status['in_reply_to_user_id_str'],'id':status['in_reply_to_user_id']}))

        else:
            
            renderedTweet = renderTweet(tweet)
            
            allTweets.append(renderedTweet)
            
            mentions, hashTags, URLs = [ tweet['entities'][field] for field in ['user_mentions','hashtags','urls'] ]
            pushEntities(mentions,hashTags,URLs,renderedTweet)
            if tweet.get('in_reply_to_status_id_str',False):
                allTweetReplies.append((renderedTweet,{'id_str':tweet['in_reply_to_status_id_str'],'id':tweet['in_reply_to_status_id']}))
            if tweet.get('in_reply_to_user_id_str',False):
                allUserReplies.append((renderedTweet,{'id_str':tweet['in_reply_to_user_id_str'],'id':tweet['in_reply_to_user_id']}))

    return {'tweets':allTweets, 'retweets':allRetweets,  'mentions':allMentions, 'tags':allHashTags, 'urls':allURLs, 'tweetReplies':allTweetReplies, 'userReplies':allUserReplies}

def tweets2Solr(tweets):
    started = datetime.now()
    addSolrDocs([ {'doc_type':'tweet', 'id':tw['id_str'], 'tweet_text':tw['text'],  'tweet_time':tw['isotime']+'Z'} for tw in tweets ])
    howLong = (datetime.now() - started).seconds
    print '*** PUSHED '+str(len(tweets))+' TWEETS TO SOLR IN '+str(howLong)+'s ***'    
    
def tweets2Neo(user,tweetDump):
    """Store a rendered set of tweets by a given user in Neo4J.
       
    Positional arguments:
    user -- screen_name of the author of the tweets
    tweetDump -- tweets, retweets, mentions, hastags, URLs and replies from "filterTweets"

    """ 

    started = datetime.now()
    rightNow = started.isoformat()

    try: # Check that a twitter user with the given screen_name exists within Neo4J.
        userNode=neoDb.find('twitter_user', property_key='screen_name', property_value=user).next()
        userNode.update_properties({'tweets_last_scraped':rightNow})
    except:
        return

    allTwits = {}
    allTweets = {}

    batch = neo4j.WriteBatch(neoDb)

    # Various Neo4J indices.
    hashIndex = neoDb.get_or_create_index(neo4j.Node, 'hashtag')
    urlIndex = neoDb.get_or_create_index(neo4j.Node, 'url')
    tweetedIndex = neoDb.get_or_create_index(neo4j.Relationship, 'tweeted')
    retweetedIndex = neoDb.get_or_create_index(neo4j.Relationship, 'retweeted')
    retweetedOfIndex = neoDb.get_or_create_index(neo4j.Relationship, 'retweet of')
    mentionIndex = neoDb.get_or_create_index(neo4j.Relationship, 'mentioned')
    tweetReplyIndex = neoDb.get_or_create_index(neo4j.Relationship, 'tweet_reply')
    userReplyIndex = neoDb.get_or_create_index(neo4j.Relationship, 'user_reply')
    taggedIndex = neoDb.get_or_create_index(neo4j.Relationship, 'tagged')
    linkedIndex = neoDb.get_or_create_index(neo4j.Relationship, 'linked')

# Adding labels to indexed nodes is broken, hence the __temp_label__ field.
# See the small footnote here: http://stackoverflow.com/questions/20010509/failed-writebatch-operation-with-py2neo

    def getTwitNode(tw):
        """Retrieve a user node from a dict, or create one in the user index and store it.""" 
        if allTwits.get(tw['id_str'],False):
            return allTwits[tw['id_str']]
        else:
            tw['last_scraped'] = rightNow
            tw['__temp_label__'] = 'twitter_user'
            abstractTwit = {}
            for k in twitterUserFields:
                if tw.get(k,False):
                    abstractTwit[k] = cypherVal(tw[k])            
            twNode = batch.get_or_create_in_index(neo4j.Node,userIndex,'id_str',tw['id_str'],abstract=abstractTwit)
            allTwits[tw['id_str']] = twNode
            return twNode    
    
    def getTweetNode(tt,retweet=False):
        """Retrieve a tweet node from a dict, or create one in the tweet index and store it.""" 
        if allTweets.get(tt['id_str'],False):
            return allTweets[tt['id_str']]
        else:
            tt['last_scraped'] = rightNow
            abstractTweet = {}
            for k in tweetFields:
                if tt.get(k,False):
                    abstractTweet[k] = cypherVal(tt[k])   
            if retweet:
                abstractTweet['__temp_label__'] = 'retweet'
                ttNode = batch.get_or_create_in_index(neo4j.Node,retweetIndex,'id_str',tt['id_str'], abstract=abstractTweet)
            else:
                abstractTweet['__temp_label__'] = 'tweet'
                ttNode = batch.get_or_create_in_index(neo4j.Node,tweetIndex,'id_str',tt['id_str'], abstract=abstractTweet)            
                                   
            allTwits[tt['id_str']] = ttNode
            return ttNode

    # Create a "TWEETED" relationship between the user andf the tweet.
    for tweet in tweetDump['tweets']:
        tweetNode = getTweetNode(tweet) 
        connlabel = user+' tweeted '+tweet['id_str']
        batch.get_or_create_indexed_relationship(tweetedIndex,'tweeted',connlabel,userNode,'TWEETED',tweetNode)

    """
    Create relationships:
        (orig_user)-[:TWEETED]->(tweet) (orig_user)-[:TWEETED]->(tweet)
        (retweet)-[:IS RETWEET OF]->(orig_tweet)
        
    """
    for retweet in tweetDump['retweets']:
        status,rt = retweet
        retweetNode = getTweetNode(rt,retweet=True)
        tweetNode = getTweetNode(status)
        authorNode = getTwitNode(status['user'])
        connlabel = status['user']['screen_name']+' tweeted '+status['id_str']
        batch.get_or_create_indexed_relationship(tweetedIndex,'tweeted',connlabel,authorNode,'TWEETED',tweetNode)
        batch.get_or_create_indexed_relationship(retweetedOfIndex,'retweet_of',tweet['id_str']+'retweet of '+status['id_str'],retweetNode,'IS RETWEET OF',tweetNode)
        batch.create_path(retweetNode,'RETWEET OF',tweetNode)        
        
    for mention in tweetDump['mentions']:
        # Create relationship (tweet)-[:MENTIONS]->(user)
        tweet,twit = mention
        tweetNode = getTweetNode(tweet)
        twitNode = getTwitNode(twit)        
        batch.get_or_create_indexed_relationship(mentionIndex,'mentions',tweet['id_str']+' mentions '+twit['screen_name'],tweetNode,'MENTIONS',twitNode)        

    hashNodes = {}
    for hashTag in tweetDump['tags']:
        tweet,tag = hashTag
        tweetNode = getTweetNode(tweet)
        hashNode = hashNodes.get(tag['text'],False)
        if not hashNode:
            hashNode = batch.get_or_create_in_index(neo4j.Node,hashIndex,'text',tag['text'],abstract={'text':tag['text'],'__temp_label__':'hashtag'})
            hashNodes[tag['text']] = hashNode
        
        # Create relationship (tweet)-[:TAGGED]->(hashtag)
        batch.get_or_create_indexed_relationship(taggedIndex,'tagged',tweet['id_str']+' tagged with '+tag['text'],tweetNode,'TAGGED',hashNode) 
        
    urlNodes = {}
    for URL in tweetDump['urls']:
        tweet,url = URL
        tweetNode = getTweetNode(tweet)       
        urlNode = urlNodes.get(url['url'],False)
        if not urlNode:
            urlNode = batch.get_or_create_in_index(neo4j.Node,urlIndex,'url',url['url'],abstract={'url':url['url'],'expanded_url':url['expanded_url'],'__temp_label__':'url'})
            urlNodes[url['url']] = urlNode
        
        # Create relationship (tweet)-[:LINKS]->(url)
        batch.get_or_create_indexed_relationship(linkedIndex,'linked',tweet['id_str']+' links to '+url['url'],tweetNode,'LINKS',urlNode)

    for tweetReply in tweetDump['tweetReplies']:
        reply, tweet = tweetReply
        tweetNode = getTweetNode(tweet)
        replyNode = getTweetNode(reply)
        # Create relationship (reply_tweet)-[:IN REPLY TO]->(tweet)
        batch.get_or_create_indexed_relationship(tweetReplyIndex,'tweet_reply','reply to '+tweet['id_str'],replyNode,'IN REPLY TO',tweetNode)

    for userReply in tweetDump['userReplies']:
        tweet, twit = userReply
        tweetNode = getTweetNode(tweet)
        replyNode = getTwitNode(twit)
        # Create relationship (user)-[:REPLIES TO]->(tweet)
        batch.get_or_create_indexed_relationship(userReplyIndex,'user_reply','reply to '+twit['id_str'],tweetNode,'REPLIES TO',replyNode)
        
    batchDone = False
    while not batchDone:
        try:
            batch.submit()
            batchDone = True
        except:
            print "*** CAN'T SUBMIT BATCH. RETRYING ***"
 
    # Adding labels to indexed nodes is broken, hence the __temp_label__ field.
    # See the small footnote here: http://stackoverflow.com/questions/20010509/failed-writebatch-operation-with-py2neo
    # Attach the proper labels in a seperate Cypher query.
    fixQueries = ['MATCH (n {__temp_label__:"twitter_user"}) WITH n SET n:twitter_user REMOVE n.__temp_label__',
        'MATCH (n {__temp_label__:"tweet"}) WITH n SET n:tweet REMOVE n.__temp_label__',
        'MATCH (n {__temp_label__:"hashtag"}) WITH n SET n:hashtag REMOVE n.__temp_label__',
        'MATCH (n {__temp_label__:"url"}) WITH n SET n:url REMOVE n.__temp_label__']
    
    for queryStr in fixQueries:
        fixedLabels = False
        while not fixedLabels:
            try:
                query = neo4j.CypherQuery(neoDb,queryStr)
                query.execute()
                fixedLabels = True
            except:
                print "*** CAN'T SET LABELS. RETRYING ***" 

    howLong = (datetime.now() - started).seconds
    print '*** '+user+': PUSHED '+str(len(tweetDump['tweets']))+' TWEETS TO NEO IN '+str(howLong)+'s ***'

def cassInsert(table,fields,values):
    quotedValues = quotedValues = [ str(v) if isinstance(v,(int,long,bool)) else "'"+unicode(re.sub("'","''",v))+"'" for v in values ]
    return ' INSERT INTO '+table+' ('+', '.join(fields)+') VALUES ('+', '.join(quotedValues)+');'

def tweets2Cass(user,tweetDump):

    started = datetime.now()

    cassCluster = Cluster()
    cassSession = cassCluster.connect()
    cassSession.set_keyspace('socialminer')

    try:
        userIdStr = cassSession.execute("SELECT id_str from twitter_user_lookup WHERE screen_name='"+user+"';")[0][0]
    except:
        return 

    def unpackTweet(tw):
        tw['user_id_str'] = tw['user']['id_str']
        twKeys, twValues = zip(*[ (key,tw[key]) for key in cassTweetFields if tw.get(key,False) ])
        return twKeys,twValues
        
    query = 'BEGIN BATCH'
    
    userTweetFields = ['user_id_str','isotime','id_str']
    retweetByFields = ['rt_user_id_str','user_id_str','isotime','id_str','rt_id_str']
    retweetOfFields = ['user_id_str','rt_user_id_str','isotime','id_str','rt_id_str']
    mentionsFields = ['target_id_str','user_id_str','tweet_id_str','isotime']
    taggedTweetFields = ['text','user_id_str','tweet_id_str','isotime']
    twitterLinkFields = ['url','expanded_url','user_id_str','tweet_id_str','isotime']
    twitterReplyFields = ['reply_id_str','reply_user_id_str','isotime','id_str','user_id_str'] 
 
    for tweet in tweetDump['tweets']:
        tweetKeys,tweetValues = unpackTweet(tweet)
        query += cassInsert('tweet',tweetKeys,tweetValues)
        query += cassInsert('user_tweet',userTweetFields,[ tweet[f] for f in userTweetFields ])
        
    for retweet in tweetDump['retweets']:
        status,rt = retweet
        
        tweetKeys,tweetValues = unpackTweet(status)
        query += cassInsert('tweet',tweetKeys,tweetValues)
        query += cassInsert('user_tweet',userTweetFields,[ tweet[f] for f in userTweetFields ])
        
        query += cassInsert('retweet_by',retweetByFields,[rt['user']['id_str'],status['user']['id_str'],rt['isotime'],status['id_str'],rt['id_str']])
        query += cassInsert('retweet_of',retweetByFields,[status['user']['id_str'],rt['user']['id_str'],rt['isotime'],status['id_str'],rt['id_str']])

    for mention in tweetDump['mentions']:
        tweet,twit = mention
        mentionVals = [tweet['user']['id_str'],userIdStr,tweet['id_str'],tweet['isotime']]
        query += cassInsert('twitter_mentions_of',mentionsFields,mentionVals)
        query += cassInsert('twitter_mentions_by',mentionsFields,mentionVals)
                
    for hashTag in tweetDump['tags']:
        tweet,tag = hashTag        
        tagVals = [tag['text'],tweet['user']['id_str'],tweet['id_str'],tweet['isotime']]
        query += cassInsert('tagged_tweet',taggedTweetFields,tagVals)
        query += cassInsert('user_tagged_tweet',taggedTweetFields,tagVals)
        
    for URL in tweetDump['urls']:
        tweet,url = URL
        linkVals = [url['url'],url['expanded_url'],tweet['user']['id_str'],tweet['id_str'],tweet['isotime']]
        query += cassInsert('twitter_links_by',twitterLinkFields,linkVals)
        query += cassInsert('twitter_links_of',twitterLinkFields,linkVals)
    
    combinedReplies = {}
   
    for tweetReply in tweetDump['tweetReplies']:
        reply, tweet = tweetReply
        combinedReplies[reply['id_str']] = {'reply_user_id_str':reply['user']['id_str'], 'isotime':reply['isotime'], 'id_str':tweet['id_str']}
        
    for userReply in tweetDump['userReplies']:
        tweet, twit = userReply
        if combinedReplies.get(tweet['id_str'],False):
            combinedReplies[tweet['id_str']]['user_id_str'] = twit['id_str']
            
    for replyIdStr in combinedReplies.keys():
        try:
            thisReply = combinedReplies[replyIdStr]
            replyVals = [replyIdStr] + [ thisReply[f] for f in twitterReplyFields[1:] ]
            query += cassInsert('twitter_replies_to',twitterReplyFields,replyVals)
            query += cassInsert('twitter_replies_by',twitterReplyFields,replyVals)
        except:
            pass
        
    query += ' APPLY BATCH'
    cassSession.execute(query)    

    howLong = (datetime.now() - started).seconds
    print '*** '+user+': PUSHED '+str(len(tweetDump['tweets']))+' TWEETS TO CASSANDRA IN '+str(howLong)+'s ***'


def uniqueNeoRelation(a,b,rel):
    return u'CREATE UNIQUE ('+a+u')-[:`'+rel+u'`]->('+b+u')'

                
def pushConnections2Neo(user, renderedTwits, friends=True):

    started = datetime.now()
    rightNow = started.isoformat()

    try:
        userNode = neoDb.find('twitter_user', property_key='screen_name', property_value=user).next()
    except:
        return

    batch = neo4j.WriteBatch(neoDb)

    if friends:
        job = ' FRIENDS'
        userNode.update_properties({'friends_last_scraped':rightNow})
        connlabel = lambda a: user+' befriended '+a['screen_name']
        link = lambda n: batch.get_or_create_indexed_relationship(friendIndex,'friends',connlabel(twit),userNode,'FOLLOWS',n)
        
    else:
        job = ' FOLLOWERS'
        userNode.update_properties({'followers_last_scraped':rightNow})
        connlabel = lambda a: a['screen_name']+' befriended '+user
        link = lambda n: batch.get_or_create_indexed_relationship(friendIndex,'friends',connlabel(twit),n,'FOLLOWS',userNode)

    for twit in renderedTwits:
        twit['last_scraped'] = rightNow
        twit['__temp_label__'] = 'twitter_user'
        
        link(batch.get_or_create_in_index(neo4j.Node, userIndex, 'id_str', twit['id_str'], abstract = twit))

    batchDone = False
    while not batchDone:
        try:
            batch.submit()
            batchDone = True
        except:
            print "*** NEO: CAN'T SUBMIT BATCH. RETRYING ***"
           

    fixedLabels = False
    while not fixedLabels:
        try:
            query = neo4j.CypherQuery(neoDb,'MATCH (n {__temp_label__:"twitter_user"}) WITH n SET n:twitter_user REMOVE n.__temp_label__')
            query.execute()
            fixedLabels = True
        except:
            print "*** NEO: CAN'T SET LABELS. RETRYING ***"
 
            
    howLong = (datetime.now() - started).seconds
    print '*** '+user+': PUSHED '+str(len(renderedTwits))+job+' TO NEO IN '+str(howLong)+'s ***'
        
def pushConnections2Cass(user, renderedTwits, friends=True):

    started = datetime.now()

    cassCluster = Cluster()
    cassSession = cassCluster.connect()
    cassSession.set_keyspace('socialminer')

    try:
        userIdStr = [cassSession.execute("SELECT id_str from twitter_user_lookup WHERE screen_name='"+user+"';")[0][0]]
    except:
        print ""
        return
    
    pushUsers2Cass(renderedTwits,cassSession)

    if friends:
        table = 'twitter_user_friends (id_str,friend_id_str,friend_screen_name) '
        job = ' FRIENDS'
        
    else:
        table = 'twitter_user_followers (id_str,follower_id_str,follower_screen_name) '
        job = ' FOLLOWERS'

    query = 'BEGIN BATCH'
    for twit in renderedTwits:
        twitValues = [ "'"+v+"'" for v in userIdStr+[twit['id_str'],twit['screen_name']] ]
        query += " INSERT INTO "+table+" VALUES ("+ ', '.join(twitValues)+");"
    query += ' APPLY BATCH'
    
    cassSession.execute(query)

    howLong = (datetime.now() - started).seconds
    print '*** '+user+': PUSHED '+str(len(renderedTwits))+job+' TO CASSANDRA IN '+str(howLong)+'s ***'

def nextFriends(latest=False):
    """ Return a list of non-supernode users who have fewer friend relationships than Twitter thinks they should."""
    desc = ' DESC' if latest else ''
    query = neo4j.CypherQuery(neoDb,'MATCH (a:twitter_user)-[:FOLLOWS]-(b:twitter_user) WITH a, COUNT(*) as c\n'
    +'WHERE c < a.friends_count/2 AND a.friends_count < 1000 AND a.followers_count < 1000 AND NOT has (a.protected) AND NOT HAS (a.defunct)\n'
    +'RETURN a.screen_name\n'
    +'ORDER BY a.last_scraped'+desc+'\n'
    +'LIMIT 20')
    return [ i.values[0] for i in query.execute().data ]

def nextFollowers(latest=False):
    """ Return a list of non-supernode users who have fewer follower relationships than Twitter thinks they should."""
    desc = ' DESC' if latest else ''
    query = neo4j.CypherQuery(neoDb,'MATCH (b:twitter_user)-[:FOLLOWS]-(a:twitter_user) WITH a, COUNT(*) as c\n'
    +'WHERE c < a.followers_count/2 AND a.followers_count < 1000 AND a.friends_count < 1000 AND NOT has (a.protected) AND NOT HAS (a.defunct)\n'
    +'RETURN a.screen_name\n'
    +'ORDER BY a.last_scraped'+desc+'\n'
    +'LIMIT 20')
    return [ i.values[0] for i in query.execute().data ]

def nextTweets(latest=False):
    """ Return a list of non-supernode users who have fewer tweets than Twitter thinks they should."""
    desc = ' DESC' if latest else ''
    query = neo4j.CypherQuery(neoDb,'MATCH (a:twitter_user) WHERE NOT (a)-[:TWEETED]->(:tweet) WITH a, COUNT(*) as c\n'
    +'WHERE c < a.statuses_count AND c < 3000 AND a.followers_count < 1000 AND a.friends_count < 1000 AND NOT has (a.protected) AND NOT HAS (a.defunct)\n'                          
    +'RETURN a.screen_name\n'
    +'ORDER BY a.last_scraped'+desc+'\n'
    +'LIMIT 20')
    return [ i.values[0] for i in query.execute().data ]

def whoNext(job,latest=False):
    """Find the next user to retrieve friends, followers or tweets, closest to the initial seed of the network."""
    if job == 'friends':
        victimGetter = nextFriends
        
    if job == 'followers':
        victimGetter = nextFollowers

    if job == 'tweets':
        victimGetter = nextTweets
          
    victimList = False
    while not victimList:
        try:
            victimList = victimGetter(latest=latest)
        except:
            pass
        
    return victimList[0]

def nextNearest(user,job,test=False):
    """Find the next user to retrieve friends, followers or tweets, closest to a given user."""
    cacheKey = '_'.join(['nextnearest',job,user])
    nextUserDump = cache.get(cacheKey)
    nextUsers = False
    if nextUserDump:
        try:
            nextUsers = json.loads(nextUserDump)
        except:
            nextUsers = []
    if nextUsers:
        print '*** NEXT '+job+': '+', '.join(nextUsers)+' ***'
        nextUser = nextUsers.pop(0)
        cache.set(cacheKey,json.dumps(nextUsers))
        return nextUser
    
    queryStr = ('MATCH (a:twitter_user{{screen_name:"'+user+'"}})-[:FOLLOWS]-(d:twitter_user)').format()+'\n'
    queryStr += ' MATCH (b:twitter_user)-[:FOLLOWS]-(d) WITH DISTINCT b '
    if job == 'friends':
        queryStr += 'MATCH (b)-[:FOLLOWS]->(c:twitter_user) '
    if job == 'followers':
        queryStr += 'MATCH (b)<-[:FOLLOWS]-(c:twitter_user) '
    if job == 'tweets':
        queryStr += 'MATCH (b)-[:TWEETED]->(c:tweet) '
    queryStr += 'WITH b, COUNT(c) AS n\n'     
    queryStr += 'WHERE b.friends_count < 1000 AND b.followers_count < 1000 AND NOT has (b.protected) AND NOT HAS (b.defunct) '
    if job == 'friends':
        queryStr += 'AND n < b.friends_count/2\n'
    if job == 'followers':
        queryStr += 'AND n < b.followers_count/2\n'
    if job == 'tweets':
        queryStr += 'AND b.statuses_count > 0 AND n < b.statuses_count/2 AND n<1000 '
    queryStr += 'RETURN b.screen_name ORDER BY b.'+job+'_last_scraped LIMIT 20'

    print '*** Looking for '+job+' ***'
 
    if test:
        return queryStr
    
    query = neo4j.CypherQuery(neoDb,queryStr)
    try:
        nextUsers = [ i.values[0] for i in query.execute().data ]
    except:
        nextUsers = []
    
    if nextUsers:
        print '*** NEXT '+job+': '+', '.join(nextUsers)+' ***'
        nextUser = nextUsers.pop(0)
        cache.set(cacheKey,json.dumps(nextUsers))
        return nextUser
    else:
        print 'No more '+job+' for '+user
    
    return False


#{u'attributes': {},
# u'bounding_box': {u'coordinates': [[[-0.14205790000000001, 51.5185518],
#    [-0.076305, 51.5185518],
#    [-0.076305, 51.575300999999996],
#    [-0.14205790000000001, 51.575300999999996]]],
#  u'type': u'Polygon'},
# u'contained_within': [],
# u'country': u'United Kingdom',
# u'country_code': u'GB',
# u'full_name': u'Islington, London',
# u'id': u'9c37c76f5dfad0fa',
# u'name': u'Islington',
# u'place_type': u'city',
# u'url': u'https://api.twitter.com/1.1/geo/id/9c37c76f5dfad0fa.json'}