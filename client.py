#!/usr/bin/python

from twisted.internet.protocol import Protocol, ReconnectingClientFactory
from twisted.protocols import basic
from twisted.internet import reactor

import re
import sys

sys.path.append('lib')

from legion.log import log

class LegionClient(basic.LineReceiver):
    delimiter = '\n'
    render_re = re.compile(r'/jobs/([^/]+)/tasks/([^/]+)/render')

    def connectionMade(self):
        self.sendLine('status')
        self.sendLine('new_job jobs/dummy/dummy.job')

    def sendLine(self, line):
        print "<<< %s" % (line,)
        basic.LineReceiver.sendLine(self, line)

    def lineReceived(self, line):
        print ">>> %s" % (line,)

        if line.startswith('#'): return
        (method, path, content) = line.split(None, 2)
        #print method, path, content
        m = self.render_re.match(path)

        if m:
            print m.groups()
            jobid, taskid = m.groups()
            self.render_task(content, jobid=jobid, taskid=taskid)

    def render_task(self, content, **kwargs):
        jobid, frame = int(kwargs['jobid']), int(kwargs['taskid'])
        self.sendLine('set_task_status %d %d complete' %
            (jobid, frame))

class LegionClientFactory(ReconnectingClientFactory):
    def startedConnecting(self, connector):
        print 'Started to connect.'

    def buildProtocol(self, addr):
        print 'Connected.'
        print 'Resetting reconnection delay'
        self.resetDelay()
        return LegionClient()

    def clientConnectionLost(self, connector, reason):
        print 'Lost connection.  Reason:', reason
        ReconnectingClientFactory.clientConnectionLost(self, connector, reason)

    def clientConnectionFailed(self, connector, reason):
        print 'Connection failed. Reason:', reason
        ReconnectingClientFactory.clientConnectionFailed(self, connector,
                                                         reason)
reactor.connectTCP('localhost', 4200, LegionClientFactory())
reactor.run()

