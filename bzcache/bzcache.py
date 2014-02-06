# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at http://mozilla.org/MPL/2.0/.

import datetime
import json
import urllib

from mozautoeslib import ESLib

import config

class BugzillaCache(object):

  def __init__(self, logger=None, es_server=config.DEFAULT_ES_SERVER,
               bzapi_server=config.DEFAULT_BZAPI_SERVER):
    self.bzapi_server = bzapi_server
    if self.bzapi_server[-1] != '/':
      self.bzapi_server += '/'
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

    if not 'ok' in result or not result['ok'] or not '_id' in result:
      raise Exception(json.dumps(result))

    return result['_id']

  def index_bugs_by_keyword(self, keyword):
    # only look at bugs that have been updated in the last 6 months
    ago = datetime.date.today() - datetime.timedelta(days=180)
    apiURL = self.bzapi_server + "bug?keywords=%s&include_fields=id,summary,status,whiteboard&changed_after=%s" % (keyword, ago.strftime('%Y-%m-%d'))
    jsonurl = urllib.urlopen(apiURL)
    buginfo = jsonurl.read()
    jsonurl.close()
    bugdict = []
    bugdict = json.loads(buginfo)['bugs']
    for bug in bugdict:
      self.add_or_update_bug(bug['id'],
                             bug['status'],
                             bug['summary'],
                             bug['whiteboard'],
                             False)

  def _get_bugzilla_data(self, bugid_array):
    buginfo = {}
    retVal = {}

    apiURL = (self.bzapi_server + "bug?id=" + ','.join(bugid_array) +
              "&include_fields=id,summary,status,whiteboard")

    jsonurl = urllib.urlopen(apiURL)
    buginfo = jsonurl.read()
    jsonurl.close()
    bugdict = []
    bugdict = json.loads(buginfo)['bugs']
    for bug in bugdict:
        retVal[bug['id']] = bug
    return retVal

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
      bzbugs = self._get_bugzilla_data(list(bugset))
      for bzbug in bzbugs:
        bug_whiteboard = bzbugs[bzbug].get('whiteboard', '')
        bugs.update({bzbug: {
                      'id': bzbug,
                      'status': bzbugs[bzbug]['status'],
                      'summary': bzbugs[bzbug]['summary'],
                      'whiteboard': bug_whiteboard
                    }})
        self.add_or_update_bug(bzbugs[bzbug]['id'],
                               bzbugs[bzbug]['status'],
                               bzbugs[bzbug]['summary'],
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
