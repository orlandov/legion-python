#!/usr/bin/python 

import sys

from twisted.application import internet, service
from twisted.internet import protocol, reactor, defer
from twisted.protocols import basic

sys.path.append('lib')

from legion.client import Client
from legion.log import log 
from legion.master import Master

class MasterProtocol(basic.LineReceiver):
    delimiter = '\n'

    def connectionMade(self):
        log.msg("New client connected")
        client = Client(self)
        self.client_id = client.id
        self.factory.master.add_client(client)

    def connectionLost(self, reason):
        log.msg("Connection to client %d lost" % (self.client_id,))
        self.factory.master.remove_client(self.client_id)

    def lineReceived(self, line):
        self.factory.master.handle_line(self.client_id, line)

class MasterService(service.Service):
    def master_factory(self):
        f = protocol.ServerFactory()
        f.master = Master()

        def schedule_dispatch_idle_clients():
            f.master.dispatch_idle_clients()
            reactor.callLater(5, schedule_dispatch_idle_clients)

        schedule_dispatch_idle_clients()
        f.protocol = MasterProtocol
        return f

master_port = 4200

application = service.Application('legion-master')
m = MasterService()
service_collection = service.IServiceCollection(application)
internet.TCPServer(master_port, m.master_factory()).setServiceParent(service_collection)
