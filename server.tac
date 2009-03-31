#!/usr/bin/python 

import re
import sys

from twisted.application import internet, service
from twisted.internet import protocol, reactor, defer
from twisted.protocols import basic

sys.path.append('lib')

from legion.client import Client
from legion.log import log 
from legion.jobs import Job, Jobs

"""
>>> Welcome client 0
<<< status
>>> 1 clients in pool
<<< start
>>> 
"""

class Master(object):
    def __init__(self):
        self.clients = {}
        self.jobs = Jobs()

    def add_client(self, client):
        self.clients[client.id] = client
        client.send_line("Welcome client %d" % (client.id))
        log.msg("Adding client %d" % (client.id))
        log.msg("%d clients currently in pool" % len(self.clients))

    def remove_client(self, id):
        del self.clients[id]

    def clients(self):
        return self.clients

    def get_client(self, id):
        return self.clients[id]

    def idle_clients(self):
        log.msg("Looking for idle clients in pool")
        log.msg("%s" % (self.clients,))
        idle = [
            self.clients[id]
            for id in self.clients
            if self.clients[id].is_idle()
        ]
        log.msg("Found %d idle clients" % (len(idle)))
        return idle

    def check_idle_clients(self):
        log.msg("Check for clients that need work")
        idle = self.idle_clients()
        active_job = self.jobs.active_job()
        if not active_job: return

        for client in idle:
            cmd = self.jobs.get_next_step_for(active_job, client)
            if (active_job.type == 'frames'):
                client.start_job(active_job, cmd)
            elif active_job.type == 'parts':
                # parts rendering not supported yet
                pass

    def handle_line(self, client_id, line):
        client = self.get_client(client_id)
        line = line.strip()
        tokens = line.split(' ')
        log.msg(tokens)

        try:
            if len(tokens) == 0:
                raise RuntimeError("Empty line")

            cmd = tokens[0]
            cmd = re.sub(r'\W', '_', cmd)
            
            try:
                func = getattr(self, 'do_%s' % (cmd,))
                func(client, tokens[1:])
            except AttributeError:
                raise
        except Exception, e:
            client.send_line("Error: %s" % (e.__str__(),))

        self.check_idle_clients()

    def do_reset_tasks(self, client, args):
        self.jobs.reset_tasks(*args)

    def do_start(self, client, args):
        id = args[0]
        client = self.get_client(id)
        if clients.status == 'paused':
            clients.status = 'idle'

    def do_pause_client(self, client, args):
        id = args[0]
        client = self.get_client(id)
        clients.status = 'paused'

    def do_start_job(self, client, args):
        job_id = args[0]
        self.jobs[job_id].status = 'pending'

    def do_pause_job(self, client, args):
        job_id = args[0]
        self.jobs[job_id].status = 'paused'

    def do_new_job(self, client, args):
        jobinfo = args[0]
        job = Job(jobinfo)
        self.jobs.add_job(job)

    def do_ping(self, client, args):
        client.send_line("pong")

    def do_status(self, client, args):
        client.send_line("%d clients in pool" % (len(self.clients)))

class MasterProtocol(basic.LineReceiver):
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
        f.protocol = MasterProtocol
        return f

master_port = 4200

application = service.Application('legion-master')
m = MasterService()
service_collection = service.IServiceCollection(application)
internet.TCPServer(master_port, m.master_factory()).setServiceParent(service_collection)
