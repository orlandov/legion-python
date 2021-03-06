#!python

import re

from legion.client import Client
from legion.log import log
from legion.jobs import Job, Jobs
from legion.error import LegionError

class Master(object):
    def __init__(self):
        self.clients = {}
        self.jobs = Jobs()
    

    def add_client(self, client):
        self.clients[client.id] = client
        client.send_line("# Welcome client %d" % (client.id))
        log.msg("Adding client %d" % (client.id))
        log.msg("%d clients currently in pool" % len(self.clients))

    def remove_client(self, id):
        try:
            del self.clients[id]
        except KeyError:
            raise LegionError('No client with id %d' % (id,))

    def clients(self):
        return self.clients

    def get_client(self, id):
        try:
            return self.clients[id]
        except KeyError:
            raise LegionError('No client with id %d' % (id,))

    def idle_clients(self):
        log.msg("Looking for idle clients in pool")
        return (
            self.clients[id]
            for id in self.clients
            if self.clients[id].is_idle()
        )

    def dispatch_idle_clients(self):
        log.msg("Check for clients that need work")
        idle = self.idle_clients()
        active_job = self.jobs.active_job()
        if not active_job: return
        log.msg("Active job %s" % (active_job.to_hash()))
        log.msg("Active tasks %s" % ( [ t.to_hash() for t in active_job.tasks ], ))

        for client in idle:
            task = active_job.assign_next_task(client)
            client.render_task(active_job, task)

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
            client.send_line("Error: %s" % (e,))

    def check_arg_count(self, args, count):
        if len(args) != count: raise LegionError("Invalid number of arguments")

    def do_set_task_status(self, client, args):
        self.check_arg_count(args, 3)
        jobid, taskid, status = args
        jobid = int(jobid)
        taskid = int(taskid)

        job = self.jobs.get_job(jobid)
        job.set_task_status(taskid, status)
        log.msg("%s"  % ([jobid, taskid, status],))
        client.status = 'idle'

    def do_reset_tasks(self, client, args):
        self.jobs.reset_tasks(*args)

    def do_start(self, client, args):
        if len(args) != 1:
            raise LegionError("Invalid arguments")

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
        client.send_line("# %d clients in pool" % (len(self.clients)))
