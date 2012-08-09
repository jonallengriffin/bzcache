import datetime
import urllib
import json
from bzcache import BugzillaCache

from pyes import ES


ES_SERVER='buildbot-es.metrics.scl3.mozilla.com:9200'

def main():
    # delete and re-create the bzcache index, in order to nuke its contents
    es = ES([ES_SERVER])
    es.delete_index('bzcache')
    es.create_index('bzcache')

    # re-cache all [orange] bugs
    bzcache = BugzillaCache(es_server=ES_SERVER)
    bzcache.index_bugs_by_whiteboard('[orange]')

    # Now, request an API from the WOO server that will cause all the bugs
    # relevant to WOO to be re-inserted into the cache.

    # calculate the startday and endday parameters
    today = datetime.date.today()
    earlier = today - datetime.timedelta(60)
    url = "http://127.0.0.1/orangefactor/api/bybug?startday=%s&endday=%s&tree=All" % \
        (str(earlier), str(today))
    print datetime.datetime.now(), url

    # retrieve the url
    data = urllib.urlopen(url)
    data.read()

if __name__ == "__main__":
    main()
