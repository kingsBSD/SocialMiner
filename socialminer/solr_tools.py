
import json
import requests

from db_settings import solrURL

solrFields = {
    'doc_type': {'type':'string','stored':True},
    'tweet_text' : {'type':'text_en','stored':True, 'termVectors':True, 'termPositions':True, 'termOffsets':True},
    'tweet_time' : {'type':'date','stored':True}
}    

solrDateFields = [ field for field in solrFields.keys() if solrFields[field]['type'] == 'date' ]

def addSolrFields():
    for fieldName in solrFields.keys():
        resp = requests.put(solrURL+'schema/fields/'+fieldName,json.dumps(solrFields[fieldName]))
        if resp.status_code == 200:
            print 'Added field: '+fieldName
        else:
            print "Couldn't add field: '"+fieldName
            
def addSolrDocs(docs):
    for doc in docs:
        for field in solrDateFields:
            val = doc.get(field,False)
            if val:
                if val[-1] <> 'Z':
                    doc[field] += 'Z'
    resp = requests.post(solrURL+'update/json?commit=true',data=json.dumps(docs),headers = {'content-type': 'application/json'})
    if resp.status_code <> 200:
        print "*** Can't push Solr docs... ***"
        print resp.text


