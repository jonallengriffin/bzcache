# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at http://mozilla.org/MPL/2.0/.

import datetime
import json
import urllib

import config
from bzcache import BugzillaCache
from mozautoeslib import ESLib

class BugzillaLoad(object):

  def __init__(self):
    self.bzurl = config.DEFAULT_BZAPI_SERVER
    self.bzcache = BugzillaCache(logger=None)

  def _load_json_url(self, url):
    f = urllib.urlopen(url)
    data = f.read()
    f.close()
    return json.loads(data)

  def _get_components(self, product):
    url = '%scount?product=%s&x_axis_field=component' % (self.bzurl, product)
    data = self._load_json_url(url)
    components = data['x_labels']
    return components

  def _load_component(self, product, component, status):
    status.insert(0, '')
    now = datetime.datetime.now() - datetime.timedelta(days=180)
    url = '%sbug?product=%s&component=%s%s&include_fields=id,summary,status&changed_after=%s' % \
          (self.bzurl, product, urllib.quote(component), '&status='.join(status), now.strftime('%Y-%m-%d'))
    data = self._load_json_url(url)
    return data['bugs']

  def _cache_bugs(self, component, bugs):
    for bug in bugs:
      print "adding %s %s %s" % (component, bug['id'], bug['status'])
      self.bzcache.add_or_update_bug(bug['id'], bug['status'], bug['summary'], False)

  def load(self, product):
    components = self._get_components(product)

    for component in components:

      bugs = self._load_component(product, component, ['UNCONFIRMED', 'NEW', 'ASSIGNED', 'REOPENED'])
      self._cache_bugs(component, bugs)

      bugs = self._load_component(product, component, ['VERIFIED', 'CLOSED', 'RESOLVED'])
      self._cache_bugs(component, bugs)

if __name__ == '__main__':
  bzload = BugzillaLoad()
  bzload.load('mozilla.org')
