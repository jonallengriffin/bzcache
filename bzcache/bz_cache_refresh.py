# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at http://mozilla.org/MPL/2.0/.

from pyes import ES
from pyes.exceptions import ElasticSearchException

import config
from bzcache import BugzillaCache


def main(options):
    es = ES([options.es_server])
    try:
        es.create_index_if_missing('bzcache')
    except ElasticSearchException:
        # create_index_if_missing is supposed not to raise if the index
        # already existing, but with the ancient pyes / ES server versions
        # we're using it still does.
        pass

    # re-cache all intermittent-failure bugs
    bzcache = BugzillaCache(es_server=options.es_server)
    bzcache.index_bugs_by_keyword('intermittent-failure')


if __name__ == "__main__":
  import optparse
  parser = optparse.OptionParser()
  parser.add_option('--es-server', dest='es_server',
                    default=config.DEFAULT_ES_SERVER,
                    help='address of ElasticSearch server; defaults to %s' %
                    config.DEFAULT_ES_SERVER)
  options, _ = parser.parse_args()
  main(options)
