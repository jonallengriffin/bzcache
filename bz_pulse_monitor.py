from daemon import createDaemon
from mozautoeslib import ESLib
from mozillapulse import consumers

import json
import logging
import socket

class MessageHandler(object):

  def __init__(self, logger):
    self.keys = ['bug.changed.status',
                 'bug.changed.summary',
                 'bug.new']
    self.doc_type = 'bugs'
    self.logger = logger
    self.index = 'bzcache'
    self.eslib = ESLib('elasticsearch1.metrics.sjc1.mozilla.com:9200', self.index)
    self.create_index(self.index)

  def log(self, msg):
    if self.logger:
      self.logger.info(msg)

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

  def add_or_update_bug(self, bugid, status, summary):
    self.log("adding bug %s" % bugid)

    try:

      # refresh the index to make sure it's up-to-date
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

  def got_message(self, data, message):
    message.ack()

    key = data['_meta']['routing_key']

    if key in self.keys:
      try:
        bugid = data['payload']['bug']['id']
        status = data['payload']['bug']['status']
        summary = data['payload']['bug']['summary']
        self.add_or_update_bug(bugid, status, summary)
      except KeyError:
        # just ignore this message
        pass

def main():
  import optparse
  parser = optparse.OptionParser()
  parser.add_option('--pidfile', dest='pidfile',
                    help='path to file for logging pid')
  parser.add_option('--logfile', dest='logfile',
                    help='path to file for logging output')
  parser.add_option('--daemon', dest='daemon', action='store_true',
                    help='run as daemon')
  options, args = parser.parse_args()

  if options.daemon:
    createDaemon(options.pidfile, options.logfile)

  logger = None
  if options.logfile:
    logger = logging.getLogger('bzcache')
    logger.setLevel(logging.DEBUG)
    handler = logging.FileHandler(options.logfile)
    logger.addHandler(handler)

  if options.pidfile is not None:
    fp = open(options.pidfile, "w")
    fp.write("%d\n" % os.getpid())
    fp.close()

  handler = MessageHandler(logger)
  pulse = consumers.BugzillaConsumer(applabel='autolog@mozilla.com|bz_monitor_' + socket.gethostname())
  pulse.configure(topic="bug.#", callback=handler.got_message, durable=False)
  pulse.listen()

if __name__ == "__main__":
  main()
