from __future__ import absolute_import

import json
import os

from socialminer.twitter_tools import *

def readTweets(path):
    files = os.listdir(path)
    tweets = []

    for name in files: 
       
        try:
            thisFile = open('/'.join([path,name]),'rb')
            print '*** OPENED: '+name+' ***'
        except:
            continue
        
        for line in thisFile.readlines():
            try:
                thisTweet = json.loads(line)
            except:
                thisTweet = False
                
            if thisTweet:
                if not thisTweet.get('id_str',False):
                    thisTweet['id_str'] = str(int(thisTweet['id']))
                
                tweets.append(thisTweet)        

        thisFile.close()
            
    return tweets

def tweetsByUser(tweets):
    users = {}
    
    for tweet in tweets:
        thisUser = tweet['user']
        screen_name = thisUser['screen_name']
        if users.get(screen_name,False):
            users[screen_name]['tweets'].append(tweet)
        else:
            users[screen_name] = {'profile':thisUser, 'tweets':[tweet]}
            
    return users



