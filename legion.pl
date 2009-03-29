#!/usr/bin/python

# legion - a render queue manager script
# by Orlando Vazquez, 2009
# 
# - Inspired by Farmerjoe (http://farmerjoe.info/)

import asynchat
import asyncore
import ConfigParser
import cStringIO
import getopt
import logging
import re
import select
import simplejson
import socket
import sys
import time
import types

sys.path.append('lib')

from legion.jobs import Job, Jobs
from legion.client import Client
from legion.log import create_logger, log

class Master(object):
    __shared_state = {} # handy borg pattern

    def __init__(self):
        self.__dict__ = self.__shared_state
        self.__clients = {}

    def add_client(self, client):
        self.__clients[client.conn.fileno()] = client
        log.debug("Adding client with fileno %d" % (client.conn.fileno()))
        log.debug("Clients in pool: %s" % len(self.clients()))

    def clients(self):
        return self.__clients

    def get_client(self, id):
        try:
            return self.__clients[id]
        except KeyError:
            raise Exception("Invalid id %d" % id)

    def idle_slaves(self):
        log.debug("Looking for idle slaves in pool")
        idle = [
            client
            for client in self.clients()
            if client.is_slave() and client.is_idle()
        ]
        log.debug("Found %d idle slaves" % (len(idle)))
        return idle

    def check_idle_slaves(self):
        log.debug("Check for slaves that need work")
        idle = self.idle_slaves()
        active_job = self.jobs.get_active_job()
        if not active_job: return

        for slave in idle:
            cmd = self.jobs.get_next_step_for(active_job, slave)
            if (active_job.type == 'frames'):
                client.start_job(active_job, cmd)
            elif active_job.type == 'parts':
                # parts rendering not supported yet
                pass

    def handle_line(self, client, line):
        tokens = line.split(' ')

        if len(tokens) == 0:
            raise RuntimeError("Invalid command")

        cmd = tokens[0]
        cmd = re.sub(r'\W', '_', cmd)
        
        try:
            func = getattr(self, 'do_%s' % (cmd,))
            func(client, tokens[1:])
        except AttributeError:
            raise RuntimeError("Invalid command")

        #master.self.check_idle_slaves()

    def do_set_client_type(self, client, args):
        if len(args) != 1: raise RuntimeError("Invalid number of arguments")
        t = args[0]
        client.type = t
        client.write("You are now a %s\n" % (t,))

class MasterHandler(asynchat.async_chat):
    def __init__(self, conn, addr, server):
        asynchat.async_chat.__init__(self, conn)
        self.rfile = cStringIO.StringIO()
        self.wfile = cStringIO.StringIO()
        self.conn = conn
        self.addr = addr
        self.server = server
        self.master = Master()

        self.client = Client(self.conn)
        self.master.add_client(self.client)

        self.set_terminator('\n')
        self.found_terminator = self.handle_command

    def collect_incoming_data(self, data):
        self.rfile.write(data)

    def handle_command(self):
        self.rfile.seek(0)
        line = self.rfile.read()
        self.rfile.truncate(0)

        log.debug('Handling command: "%s"' % (line))
        self.master.handle_line(self.client, line)

class MasterServer(asyncore.dispatcher):
    def __init__(self, port):
        log.debug('creating MasterServer instance')
        self.port = port

        asyncore.dispatcher.__init__(self)

        self.create_socket (socket.AF_INET, socket.SOCK_STREAM)
        self.set_reuse_addr()
        self.bind (('', port))
        self.listen (1024)

    def handle_accept(self):
        log.debug('handle_accept')
        try:
            conn, addr = self.accept()
        except socket.error:
            log.critical("OMG socket error!!", exc_info=1)
            return

        MasterHandler(conn, addr, self)

    def run(self):
        asyncore.loop()


class SlaveHandler(asynchat.async_chat):
    def __init__(self, conn):
        pass

    def collect_incoming_data(self, data):
        self.rfile.write(data)

    def handle_command(self):
        self.rfile.seek(0)
        line = self.rfile.read()
        self.rfile.truncate(0) # this seems wrong
        self.handle_line(line)
        self.sockstream.write("server said: %s\n" % (line))
        self.sockstream.finish()

    def handle_line(self, line):
        print "server said: line"


class SlaveClient(object):
    def __init__(self, master, port):
        self.master = master
        self.port = port

    def run(self):
        conn.write('O HAI');


def usage():
    print """syntax: legion [options]
        --master        start a master server
        --slave         start a slave"""

def get_options():
    mode = None
    debug = 0

    try:
        opts, args = getopt.getopt(sys.argv[1:],
                "mscdh", ['master','slave','help','debug'])
        print opts, args
        for opt,optarg in opts:
            if opt == '--master':
                mode = 'master'
            elif opt == '--slave':
                mode = 'slave'
            elif opt in ['-d', '--debug']:
                debug = 1
        if not mode:
            raise getopt.GetoptError('No mode specified')

    except getopt.GetoptError:
        usage()
        sys.exit(2);

    return { 'mode': mode, 'debug': debug }

if __name__ == '__main__':
    create_logger()

    mode = None
    master_port = 4200

    options = get_options()
    if options['debug'] > 0:
        log.setLevel(options.get('debug'))
        log.info('enabling debug mode')
        log.debug('test')

    if options['mode'] == 'master':
        log.info("Starting master server on port %d" % (master_port,))
        app = MasterServer(master_port)
    elif options['mode'] == 'slave':
        app = Slave(master="localhost", master_port=master_port)

    app.run()

#config_path = 'legion.conf'
#config = ConfigParser.RawConfigParser()
#config.read(config_path)


