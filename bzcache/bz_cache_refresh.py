import datetime
import urllib

from pyes import ES


def main():
  # delete and re-create the bzcache index, in order to nuke its contents
  es = ES(['elasticsearch1.metrics.sjc1.mozilla.com:9200'])
  es.delete_index('bzcache')
  es.create_index('bzcache')

  # Now, request an API from the WOO server that will cause all the bugs
  # relevant to WOO to be re-inserted into the cache.

  # calculate the startday and endday parameters
  today = datetime.date.today()
  earlier = today - datetime.timedelta(60)
  url = "http://brasstacks.mozilla.com/orangefactor/api/bybug?startday=%s&endday=%s&tree=All" % \
    (str(earlier), str(today))
  print datetime.datetime.now(), url

  # retrieve teh url
  data = urllib.urlopen(url)
  data.read()

if __name__ == "__main__":
  main()
