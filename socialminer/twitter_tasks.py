from __future__ import absolute_import
"""Celery tasks relating to Twitter."""

__author__ = 'Giles Richard Greenway'


from datetime import datetime

from celery import chain, group

from socialminer.celery import app
from socialminer.db_settings import cache
from socialminer.twitter_settings import *
from socialminer.twitter_tools import *

@app.task
def twitterCall(methodName,args,credentials=False):
    """Attempt a given Twitter API call, retry if rate-limited. Returns the result of the call.
    
        Positional arguments:
        methodName -- the Twitter API method to call
        args -- a dicionary of keyword arguments
        
    """
    api = ratedTwitter(credentials=credentials)
    limit = api.__can_we_do_that__(methodName)
    if limit:
        print '*** TWITTER RATE-LIMITED: '+methodName+' ***'
        raise twitterCall.retry(exc=Exception('Twitter rate-limited',methodName), countdown = limit)
    else:
        okay, result = api.__method_call__(methodName,args)
        if okay:
            print '*** TWITTER CALL: '+methodName+' ***' 
            return result
        else:
            assert False

@app.task
def pushRenderedTwits2Neo(twits):
    pushUsers2Neo(twits)

@app.task
def pushRenderedTwits2Cass(twits):
    pushUsers2Cass(twits)

@app.task
def pushTwitterUsers(twits):
    """Store Twitter users returned by a Twitter API call in Neo4J and Cassandra.
    
    Positional arguments:
    twits -- a list of Twitter users as returned by Twython
    """
    rightNow = datetime.now().isoformat()
    for twit in twits:
        twit['last_scraped'] = rightNow
        
    renderedTwits = [ renderTwitterUser(twit) for twit in twits ]
    pushRenderedTwits2Neo.delay(renderedTwits)
    pushRenderedTwits2Cass.delay(renderedTwits)
    return True

@app.task
def getTwitterUsers(users,credentials=False):
    """Look-up a set of Twitter users by screen_name and store them in Neo4J/Cassandra

    Positional arguments:
    users -- a list of screen_names
    
    """
    userList = ','.join(users)
    return chain(twitterCall.s('lookup_user',{'screen_name':userList},credentials), pushTwitterUsers.s())()

@app.task
def pushRenderedTweets2Neo(user,tweetDump):
    tweets2Neo(user,tweetDump)

@app.task
def pushRenderedTweets2Cass(user,tweetDump):
    tweets2Cass(user,tweetDump)

@app.task
def pushTweets(tweets,user,cacheKey=False):
    """ Dump a set of tweets from a given user's timeline to Neo4J/Cassandra.

    Positional arguments:
    tweets -- a list of tweets as returned by Twython.
    user -- screen_name of the user
    
    Keyword arguments:
    cacheKey -- a Redis key that identifies an on-going task to grab a user's timeline
    
    """
    
    tweetDump = filterTweets(tweets) # Extract mentions, URLs, replies hashtags etc...

    pushRenderedTweets2Neo.delay(user,tweetDump) 
    pushRenderedTweets2Cass.delay(user,tweetDump)

    if cacheKey: # These are the last Tweets, tell the scaper we're done.
        cache.set(cacheKey,'done')
        print '*** '+user+': DONE WITH TWEETS ***' 
       
    return True

@app.task
def getTweets(user,maxTweets=3000,count=0,tweetId=0,cacheKey=False,credentials=False):
    """Get tweets from the timeline of the given user, push them to Neo4J/Cassandra.
    
    Positional arguments:
    user -- The screen_name of the user

    Keyword arguments:
    maxTweets -- The maximum number of tweets to retrieve
    cacheKey -- a Redis key that identifies an on-going task to grab a user's timeline
    count -- The number of tweets already retrieved, set when the task calls itself
    tweetId -- The maximum tweet ID to retrieve, set when the task calls itself
    
    """
    api = ratedTwitter(credentials=credentials)
    limit = api.get_user_timeline_limited()
    if limit:
        print '*** TWITTER RATE-LIMITED: statuses.user_timeline:'+user+':'+str(count)+' ***'
        raise getTweets.retry(countdown = limit)
    else:
        args = {'screen_name':user,'exclude_replies':False,'include_rts':True,'trim_user':False,'count':200}
        if tweetId:
            args['max_id'] = tweetId
        
        okay, result = api.get_user_timeline(**args)
        
        if okay:
            print '*** TWITTER USER_TIMELINE: '+user+':'+str(tweetId)+' ***'
            if result:
                newCount = count + len(result)
                if maxTweets:
                    if newCount > maxTweets: # No need for the task to call itself again.
                        pushTweets.delay(result,user,cacheKey=cacheKey) # Give pushTweets the cache-key to end the job.
                        return
                    else:
                        pushTweets.delay(result,user)

                newTweetId = min([t['id'] for t in result]) - 1 
                # Not done yet, the task calls itself with an updated count and tweetId.
                getTweets.delay(user,maxTweets=maxTweets,count=newCount,tweetId=newTweetId,cacheKey=cacheKey,credentials=credentials)
            else:
                pushTweets.delay([],user,cacheKey=cacheKey) # Nothing more found, so tell pushTweets the job is done.
        else:
            if result == 'limited':
                raise getTweets.retry(countdown = api.get_user_timeline_limited())

@app.task
def pushRenderedConnections2Neo(user,renderedTwits,friends=True):
    pushConnections2Neo(user,renderedTwits,friends=friends)
    
@app.task
def pushRenderedConnections2Cass(user,renderedTwits,friends=True):
    pushConnections2Cass(user,renderedTwits,friends=friends)  
                    
@app.task
def pushTwitterConnections(twits,user,friends=True,cacheKey=False):
    """Push the Twitter connections of a given user to Neo4J/Cassandra.
    
    Positional arguments:
    twits -- a list of Twitter users as returned by Twython
    user -- The screen_name of the user

    Keyword arguments:
    friends -- "twits" are the user's friends if True, (default) else they're followers 
    cacheKey -- a Redis key that identifies an on-going task to grab a user's friends or followers
    
    """

    if friends:
        job = ' FRIENDS'
    else:
        job = ' FOLLOWERS'
    
    if twits:
        renderedTwits = [ renderTwitterUser(twit) for twit in twits ]
        pushRenderedConnections2Neo.delay(user,renderedTwits,friends=friends)
        pushRenderedConnections2Cass.delay(user,renderedTwits,friends=friends)
# These are the last Tweets, tell the scaper we're done.# These are the last Tweets, tell the scaper we're done.bbbbbbb
    if cacheKey: # These are the last connections, tell the scaper we're done.
        cache.set(cacheKey,'done')
        print '*** '+user+': DONE WITH'+job+' ***' 
                   
@app.task
def getTwitterConnections(user,friends=True,cursor = -1,credentials=False,cacheKey=False):
    """Get the connections of the given user, push them to Neo4J/Cassandra.

    Positional arguments:
    user -- The screen_name of the user

    Keyword arguments:
    friends -- "twits" are the user's friends if True, (default) else they're followers 
    cacheKey -- a Redis key that identifies an on-going task to grab a user's friends or followers
    cursor -- Id of the next block of connections to retrieve, set when the task calls itself
    """
    api = ratedTwitter(credentials=credentials)
    if friends:
        method = api.get_friends_list
        limit = api.get_friends_list_limited()
        methodName = 'get_friends_list' 
    else:
        method = api.get_followers_list
        limit = api.get_followers_list_limited()
        methodName = 'get_followers_list'
    if limit:    
        print '*** TWITTER RATE-LIMITED: '+methodName+':'+str(cursor)+' ***'
        raise getTwitterConnections.retry(countdown = limit)    
    else:
        okay,result = method(screen_name=user, cursor=cursor, count=200) # We can get a maximum of 200 connections at once.       
        if okay:
            print '*** TWITTER CURSOR: '+methodName+':'+user+':'+str(cursor)+' ***'
            twits = result['users']
            nextCursor = result.get('next_cursor',False)
            if nextCursor: # Unless the next cursor is 0, we're not done yet.
                getTwitterConnections.delay(user,friends=friends,cursor=nextCursor,cacheKey=cacheKey,credentials=credentials)
                pushTwitterConnections.delay(twits,user,friends=friends)
            else:
                pushTwitterConnections.delay(twits,user,friends=friends,cacheKey=cacheKey) # All done, send the cacheKey.
                    
        else:
            if result == 'limited':
                 raise getTwitterConnections.retry(exc=Exception('Twitter rate-limited',methodName),countdown = API_TIMEOUT)    

@app.task
def startScrape(mode='default'):
      
    if mode == 'default':
        print '*** STARTED SCRAPING: DEFAULT: ***' 
        cache.set('default_scrape','true')
        cache.set('scrape_mode','default')
    
    for key in ['default_friends','default_followers','default_tweets']:
        cache.set(key,'')
    
    doDefaultScrape.delay()
          
@app.task
def doDefaultScrape(junk=False):

    keepGoing = cache.get('default_scrape')
    if (not keepGoing) or keepGoing <> 'true':
        print '*** STOPPED DEFAULT SCRAPE ***' 
        return False
    
    print '*** SCRAPING... ***'

    thisFriend = cache.get('default_friends')
    if (not thisFriend) or thisFriend == 'done':
        cache.set('default_friends','running')
        getTwitterConnections.delay(whoNext('friends'),cacheKey='default_friends')
    else:
        print '*** FRIENDS BUSY ***'

    thisFollower = cache.get('default_followers')
    if (not thisFollower) or thisFollower == 'done':
        cache.set('default_followers','running')
        getTwitterConnections.delay(whoNext('friends'),friends=False,cacheKey='default_followers')
    else:
        print "*** FOLLOWERS BUSY ***"


    thisTweet = cache.get('default_tweets')
    if (not thisTweet) or thisTweet == 'done':
        cache.set('default_tweets','running')
        getTweets.delay(whoNext('tweets'),maxTweets=1000,cacheKey='default_tweets')
    else:
        print '*** TWEETS BUSY ***'
                    
    doDefaultScrape.apply_async(countdown=30)



  
    
