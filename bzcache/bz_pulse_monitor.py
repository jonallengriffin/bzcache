from bzcache import BugzillaCache
from daemon import createDaemon
from mozillapulse import consumers

import json
import logging
import os
import socket

class MessageHandler(object):

  def __init__(self, logger):
    self.keys = ['bug.changed.status',
                 'bug.changed.summary',
                 'bug.added.whiteboard',
                 'bug.changed.whiteboard',
                 'bug.new']
    self.logger = logger
    self.bzcache = BugzillaCache(logger=self.logger)

  def log(self, msg):
    if self.logger:
      self.logger.info(msg)
    else:
      print msg

  def got_message(self, data, message):
    message.ack()

    key = data['_meta']['routing_key']
    #print key, data['payload']['bug']['id']

    if key in self.keys:
      try:
        bugid = data['payload']['bug']['id']
        status = data['payload']['bug']['status']
        if 'changed.status' in key:
          status = data['payload']['after']
        summary = data['payload']['bug']['summary']
        whiteboard = data['payload']['bug']['whiteboard']
        if 'changed.whiteboard' in key:
          whiteboard = data['payload']['after']
        elif 'added.whiteboard' in key:
          whiteboard = data['payload']['value']
        self.bzcache.add_or_update_bug(bugid, status, summary, whiteboard)
      except KeyError, inst:
        self.log('exception handling message %s' % key)
        self.log(inst)

def main():
  import optparse
  parser = optparse.OptionParser()
  parser.add_option('--pidfile', dest='pidfile',
                    help='path to file for logging pid')
  parser.add_option('--logfile', dest='logfile',
                    help='path to file for logging output')
  parser.add_option('--daemon', dest='daemon', action='store_true',
                    help='run as daemon')
  parser.add_option('--durable', dest='durable', action='store_true',
                    help='use a durable pulse consumer')
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
  pulse.configure(topic="#", callback=handler.got_message, durable=options.durable)
  pulse.listen()

if __name__ == "__main__":
  main()
