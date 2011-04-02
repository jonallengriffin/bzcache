import json

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

  def get_bugs(self, bugids):
    bugs = {}
    data = self.eslib.query({ 'bugid': tuple(bugids) },
                            doc_type=[self.doc_type])

    for bug in data:
      bugs[bug['bugid']] = {
        'status': bug['status'],
        'id': bug['bugid'],
        'summary': bug['summary']
      }

    # XXX need to identify bugs that weren't returned from the cache
    # and grab the info from Bugzilla instead

    return bugs

  def add_or_update_bug(self, bugid, status, summary, refresh=True):
    self.log("adding bug %s" % bugid)

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
               'summary': summary
             }

      # if there's already an instance of this bug in ES, update it,
      # otherwise add it
      if bug:
        id = self._add_doc(data, bug[0]['_id'])
        self.log("%s updated" % id)
      else:
        id = self._add_doc(data)
        self.log("%s added" % id)

    except Exception, inst:
      self.log(inst)
