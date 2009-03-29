#!/usr/bin/python 

import sys
from twisted.application import internet, service
from twisted.internet import protocol, reactor, defer
from twisted.protocols import basic
from twisted.python import log

sys.path.append('lib')

class MasterProtocol(basic.LineReceiver):
    def connectionMade(self):
        self.factory.clients.append(self)

    def connectionLost(self, reason):
        self.factory.clients.remove(self)

    def lineReceived(self, line):
        log.msg("got line '%s'" % (line,))
        if line == "status":
            self.send('%d clients connected' % (len(self.factory.clients),))

    def send(self, line):
        self.transport.write(line + "\n")

class MasterService(service.Service):
    def master_factory(self):
        f = protocol.ServerFactory()
        f.clients = []
        f.protocol = MasterProtocol
        return f

master_port = 4200

application = service.Application('legion-master')
m = MasterService()
service_collection = service.IServiceCollection(application)
internet.TCPServer(master_port, m.master_factory()).setServiceParent(service_collection)
