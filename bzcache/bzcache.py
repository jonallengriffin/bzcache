# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at http://mozilla.org/MPL/2.0/.

import datetime
import json

import requests
from mozautoeslib import ESLib

import config

class BugzillaCache(object):

  def __init__(self, logger=None, es_server=config.DEFAULT_ES_SERVER,
               bugzilla_api_url=config.DEFAULT_BUGZILLA_API_URL):
    self.bugzilla_api_url = bugzilla_api_url.rstrip('/')
    self.doc_type = 'bugs'
    self.index = 'bzcache'
    self.eslib = ESLib(es_server, self.index)
    self.logger = logger
    self.create_index(self.index)

  def log(self, msg):
    if self.logger:
      self.logger.info(msg)
    else:
      print msg

  def create_index(self, index):
    try:
      self.eslib.connection.open_index(index)
    except Exception:
      self.log('creating bzcache index')
      self.eslib.connection.create_index(index)

  def refresh_index(self):
    self.eslib.connection.refresh(indexes=[self.index])

  def _add_doc(self, doc, id=None):
    result = self.eslib.add_doc(doc, id, doc_type=self.doc_type)

    # ElasticSearch v1.x uses 'created', v0.9 uses 'ok'
    created = result.get('created', False) or result.get('ok', False)

    if created and '_id' in result:
      return result['_id']

    raise Exception(json.dumps(result))

  def fetch_json(self, url, params=None, timeout=30):
      self.log('Fetching %s with params %s' % (url, params))
      headers = {
          'Accept': 'application/json',
          'User-Agent': 'bzcache',
      }
      response = requests.get(url, params=params, headers=headers, timeout=timeout)
      response.raise_for_status()
      return response.json()

  def fetch_intermittent_bugs(self, offset, limit):
      url = self.bugzilla_api_url + '/bug'
      params = {
          'keywords': 'intermittent-failure',
          # only look at bugs that have been updated in the last 6 months
          'chfieldfrom': '-6m',
          'include_fields': 'id,summary,status,whiteboard',
          'offset': offset,
          'limit': limit,
      }
      results = self.fetch_json(url, params=params)
      return results.get('bugs', [])

  def index_bugs_by_keyword(self, keyword):
      bug_list = []

      offset = 0
      limit = 500

      # Keep querying Bugzilla until there are no more results.
      while True:
          bug_results_chunk = self.fetch_intermittent_bugs(offset, limit)
          bug_list += bug_results_chunk
          if len(bug_results_chunk) < limit:
              break
          offset += limit

      for bug in bug_list:
          self.add_or_update_bug(bug['id'],
                                 bug['status'],
                                 bug['summary'],
                                 bug['whiteboard'],
                                 False)

  def _get_bugzilla_data(self, bugid_array):
    # request bugs from Bugzilla in groups of 200
    chunk_size = 200
    bugs = []

    bugid_chunks = [list(bugid_array)[i:i+chunk_size]
                    for i in range(0, len(bugid_array), chunk_size)]
    for bugid_chunk in bugid_chunks:
        apiURL = (self.bugzilla_api_url + "/bug?id=" + ','.join(bugid_array) +
                  "&include_fields=id,summary,status,whiteboard")
        bugs += self.fetch_json(apiURL).get('bugs', [])
    return bugs

  def get_bugs(self, bugids):
    bugs = {}
    bugset = set(bugids)

    # request bugs from ES in groups of 250
    chunk_size = 250
    bug_chunks = [list(bugset)[i:i+chunk_size]
                  for i in range(0, len(bugset), chunk_size)]

    for bug_chunk in bug_chunks:
      data = self.eslib.query({ 'bugid': tuple(bug_chunk) },
                              doc_type=[self.doc_type])

      for bug in data:
        bugs[bug['bugid']] = {
          'status': bug['status'],
          'id': bug['bugid'],
          'summary': bug['summary'],
          'whiteboard': bug.get('whiteboard', '')
        }
        try:
          bugset.remove(str(bug['bugid']))
        except:
          pass

    if len(bugset):
      for bzbug in self._get_bugzilla_data(list(bugset)):
          bug_id = bzbug['id']
          bug_whiteboard = bzbug.get('whiteboard', '')
          bugs[bug_id] = {
              'id': bzbug['id'],
              'status': bzbug['status'],
              'summary': bzbug['summary'],
              'whiteboard': bug_whiteboard
          }
          self.add_or_update_bug(bug_id,
                                 bzbug['status'],
                                 bzbug['summary'],
                                 bug_whiteboard,
                                 False)

    return bugs

  def add_or_update_bug(self, bugid, status, summary, whiteboard, refresh=True):
    # make sure bugid is a string, for consistency
    bugid = str(bugid)

    date = datetime.datetime.now().strftime('%Y-%m-%d, %H:%M:%S')

    try:

      # refresh the index to make sure it's up-to-date
      if refresh:
        self.refresh_index()

      data = { 'bugid': bugid,
               'status': status,
               'summary': summary,
               'whiteboard': whiteboard,
             }

      id = self._add_doc(data, bugid)
      self.log("%s - %s added, status: %s, id: %s" % (date, bugid, status, id))

    except Exception, inst:
      self.log('%s - exception while processing bug %s' % (date, id))
      self.log(inst)

    return True
