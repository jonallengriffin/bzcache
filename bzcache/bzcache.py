import datetime
import json
import urllib

from mozautoeslib import ESLib

class BugzillaCache(object):

  def __init__(self, logger=None):
    self.doc_type = 'bugs'
    self.index = 'bzcache'
    self.eslib = ESLib('elasticsearch1.metrics.sjc1.mozilla.com:9200', self.index)
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

  def _get_bugzilla_data(self, bugid_array):
    buginfo = {}
    retVal = {}

    apiURL = ("https://api-dev.bugzilla.mozilla.org/latest/bug?id=" + 
              ','.join(bugid_array) + 
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
    data = self.eslib.query({ 'bugid': tuple(bugids) },
                            doc_type=[self.doc_type])

    for bug in data:
      bugs[bug['bugid']] = {
        'status': bug['status'],
        'id': bug['bugid'],
        'summary': bug['summary'],
        'whiteboard': bug.get('whiteboard')
      }
      bugset.remove(str(bug['bugid']))

    if len(bugset):
      bzbugs = self._get_bugzilla_data(list(bugset))
      bugs.update(bzbugs)
      for bzbug in bzbugs:
        self.add_or_update_bug(bzbugs[bzbug]['id'],
                               bzbugs[bzbug]['status'],
                               bzbugs[bzbug]['summary'],
                               bzbugs[bzbug].get('whiteboard'),
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

      # look for an existing bug with this id
      bug = self.eslib.query({ 'bugid': bugid },
                             doc_type=[self.doc_type],
                             withSource=True)

      data = { 'bugid': bugid,
               'status': status,
               'summary': summary,
               'whiteboard': whiteboard,
             }

      # if there's already an instance of this bug in ES, update it,
      # otherwise add it
      if bug:
        id = self._add_doc(data, bug[0]['_id'])
        self.log("%s - %s updated, old status: %s, new status: %s, id: %s" % \
                 (date, bugid, bug[0]['_source']['status'], status, id))
        if status == bug[0]['_source']['status'] and \
           summary == bug[0]['_source']['summary'] and \
           whiteboard == bug[0]['_source']['whiteboard']:
          return False
      else:
        id = self._add_doc(data)
        self.log("%s - %s added, status: %s, id: %s" % (date, bugid, status, id))

    except Exception, inst:
      self.log('%s - exception while processing bug %s' % (date, id))
      self.log(inst)

    return True
