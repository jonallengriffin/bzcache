# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at http://mozilla.org/MPL/2.0/.

import datetime
import urllib
import urlparse

from pyes import ES

import config
from bzcache import BugzillaCache


def main(options):
    # delete and re-create the bzcache index, in order to nuke its contents
    es = ES([options.es_server])
    es.delete_index('bzcache')
    es.create_index('bzcache')

    # re-cache all intermittent-failure bugs
    bzcache = BugzillaCache(es_server=options.es_server)
    bzcache.index_bugs_by_keyword('intermittent-failure')

    # Now, request an API from the WOO server that will cause all the bugs
    # relevant to WOO to be re-inserted into the cache.

    # calculate the startday and endday parameters
    today = datetime.date.today()
    earlier = today - datetime.timedelta(60)
    of_parsed_url = urlparse.urlparse(options.of_server)
    of_base_url = '%s://%s%s%s' % (of_parsed_url.scheme or 'http',
                                   of_parsed_url.netloc,
                                   of_parsed_url.path,
                                   '/' if of_parsed_url.path[-1] != '/' else '')
    of_url = "%sbybug?startday=%s&endday=%s&tree=All" % \
        (of_base_url, str(earlier), str(today))
    print datetime.datetime.now(), of_url

    # retrieve the url
    data = urllib.urlopen(of_url)
    data.read()

if __name__ == "__main__":
  import optparse
  parser = optparse.OptionParser()
  parser.add_option('--es-server', dest='es_server',
                    default=config.DEFAULT_ES_SERVER,
                    help='address of ElasticSearch server; defaults to %s' %
                    config.DEFAULT_ES_SERVER)
  parser.add_option('--of-server', dest='of_server',
                    default=config.DEFAULT_OF_SERVER,
                    help='address of OrangeFactor server; defaults to %s' %
                    config.DEFAULT_OF_SERVER)
  options, _ = parser.parse_args()
  main(options)
