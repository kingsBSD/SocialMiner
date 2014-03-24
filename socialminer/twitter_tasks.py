from __future__ import absolute_import
# Licensed under the Apache License Version 2.0: http://www.apache.org/licenses/LICENSE-2.0.txt
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
    #return True

@app.task
def getTwitterUsers(users,credentials=False):
    """Look-up a set of Twitter users by screen_name and store them in Neo4J/Cassandra

    Positional arguments:
    users -- a list of screen_names
    
    """
    userList = ','.join(users)
    chain(twitterCall.s('lookup_user',{'screen_name':userList},credentials), pushTwitterUsers.s())()

@app.task
def pushRenderedTweets2Neo(user,tweetDump):
    tweets2Neo(user,tweetDump)

@app.task
def pushRenderedTweets2Cass(user,tweetDump):
    tweets2Cass(user,tweetDump)
    
@app.task
def pushRenderedTweets2Solr(tweets):
    tweets2Solr(tweets)    

@app.task
def pushTweets(tweets,user,cacheKey=False):
    """ Dump a set of tweets from a given user's timeline to Neo4J/Cassandra/Solr.

    Positional arguments:
    tweets -- a list of tweets as returned by Twython.
    user -- screen_name of the user
    
    Keyword arguments:
    cacheKey -- a Redis key that identifies an on-going task to grab a user's timeline
    
    """
    
    tweetDump = filterTweets(tweets) # Extract mentions, URLs, replies hashtags etc...

    pushRenderedTweets2Neo.delay(user,tweetDump) 
    pushRenderedTweets2Cass.delay(user,tweetDump)
    pushRenderedTweets2Solr.delay(tweetDump['tweets']+tweetDump['retweets'])

    if cacheKey: # These are the last Tweets, tell the scaper we're done.
        cache.set(cacheKey,'done')
        print '*** '+user+': DONE WITH TWEETS ***' 
       
    #return True

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
            if result == '404':
                setUserDefunct(user)
            cache.set('scrape_tweets','done')
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
# These are the last Tweets, tell the scaper we're done.
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
            if result == '404':
                setUserDefunct(user)
                if friends:
                    cache.set('scrape_friends','done')
                else:
                    cache.set('scrape_followers','done')

@app.task
def seedUser(user,scrape=False):
    """Retrieve the given Twitter user's account, and their timelines, friends and followers. Optionally, start scraping around them."""
    print '*** SEEDING: '+user+' ***'
    if scrape:
        chain(getTwitterUsers.s([user]),getTwitterConnections.si(user), getTwitterConnections.si(user,friends=False), 
              getTweets.si(user,maxTweets=1000), startUserScrape.si(user))()
    else:
        chain(getTwitterUsers.s([user]),getTwitterConnections.si(user), getTwitterConnections.si(user,friends=False),
              getTweets.si(user,maxTweets=1000))()

@app.task
def startScrape(latest=False):
    """Start the default scrape, retrieving the users that need timelines, friends or followers updated,
    in the order that they were first added. """
    print '*** STARTED SCRAPING: DEFAULT: ***' 
    cache.set('default_scrape','true')
    cache.set('scrape_mode','default')
    
    for key in ['scrape_friends','scrape_followers','scrape_tweets']:
        cache.set(key,'')
    
    doDefaultScrape.delay(latest=latest)
          
@app.task
def doDefaultScrape(latest=False):
    """Retrieve the tweets, friends or followers of trhe next users in the default scrape."""
    keepGoing = cache.get('default_scrape')
    if (not keepGoing) or keepGoing <> 'true':
        print '*** STOPPED DEFAULT SCRAPE ***' 
        return False
    
    print '*** SCRAPING... ***'

    thisFriend = cache.get('scrape_friends')
    if (not thisFriend) or thisFriend == 'done':
        cache.set('scrape_friends','running')
        getTwitterConnections.delay(whoNext('friends',latest=latest),cacheKey='scrape_friends')
    else:
        print '*** FRIENDS BUSY ***'

    thisFollower = cache.get('scrape_followers')
    if (not thisFollower) or thisFollower == 'done':
        cache.set('scrape_followers','running')
        getTwitterConnections.delay(whoNext('friends',latest=latest),friends=False,cacheKey='scrape_followers')
    else:
        print "*** FOLLOWERS BUSY ***"

    thisTweet = cache.get('scrape_tweets')
    if (not thisTweet) or thisTweet == 'done':
        cache.set('scrape_tweets','running')
        getTweets.delay(whoNext('tweets',latest=latest),maxTweets=1000,cacheKey='scrape_tweets')
    else:
        print '*** TWEETS BUSY ***'
                    
    doDefaultScrape.apply_async(kwargs={'latest':latest},countdown=30)

@app.task
def startUserScrape(user):
    """Start scraping around the given user.""" 
    print '*** STARTED SCRAPING: USER: '+user+' ***'
    cache.set('user_scrape','true')
    cache.set('scrape_mode','user')
    cache.set('scrape_user',user)

    for key in ['scrape_friends','scrape_followers','scrape_tweets']:
        cache.set(key,'')
        
    for job in ['friends','followers','tweets']:
        cacheKey = '_'.join(['nextnearest',job,user])
        cache.set(cacheKey,False)     
        
    doUserScrape.delay()
    
@app.task
def doUserScrape():
    """Retrieve the next timelines, friends and followers for the next accounts in the user scrape. """
    keepGoing = cache.get('user_scrape')
    if (not keepGoing) or keepGoing <> 'true':
        print '*** STOPPED USER SCRAPE ***' 
        return False
    
    user = cache.get('scrape_user')
    print '*** SCRAPING USER: '+user+'... ***'

    thisFriend = cache.get('scrape_friends')
    if (not thisFriend) or thisFriend == 'done':
        nextFriends = nextNearest(user,'friends')
        if nextFriends:
            cache.set('scrape_friends','running')
            getTwitterConnections.delay(nextFriends, cacheKey='scrape_friends')
    else:
        print '*** FRIENDS BUSY ***'

    thisFollower = cache.get('scrape_followers')
    if (not thisFollower) or thisFollower == 'done':
        nextFollowers = nextNearest(user,'followers')
        if nextFollowers:
            cache.set('scrape_followers','running')
            getTwitterConnections.delay(nextFollowers,friends=False, cacheKey='scrape_followers')
    else:
        print '*** FOLLOWERS BUSY ***'

    thisTweet = cache.get('scrape_tweets')
    if (not thisTweet) or thisTweet == 'done':
        nextTweets = nextNearest(user,'tweets')
        if nextTweets:
            cache.set('scrape_tweets','running')
            getTweets.delay(nextTweets,maxTweets=1000,cacheKey='scrape_tweets')
    else:
        print '*** TWEETS BUSY ***'

    if 'running' in [ cache.get(k) for k in ['scrape_friends','scrape_followers','scrape_tweets'] ]:
        doUserScrape.apply_async(countdown=30)
    else:
        cache.set('user_scrape','')
        cache.set('scrape_mode','')        
        print '*** FINISHED SCRAPING USER: '+user+' ***'