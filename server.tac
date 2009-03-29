#!/usr/bin/python 

import sys
from twisted.application import internet, service
from twisted.internet import protocol, reactor, defer
from twisted.protocols import basic

sys.path.append('lib')
from legion.log import create_logger, log

class MasterProtocol(basic.LineReceiver):
    def lineReceived(self, user):
        self.transport.write('sup bitch')

class MasterService(service.Service):
    def __init__(self):
        pass

    def master_factory(self):
        f = protocol.ServerFactory()
        f.protocol = MasterProtocol
        return f

if __name__ == '__main__':
    create_logger()

    master_port = 4200

    application = service.Application('legion-master')
    m = MasterService()
    service_collection = service.IServiceCollection(application)
    internet.TCPServer(master_port, m.master_factory()).setServiceParent(service_collection)
