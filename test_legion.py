#!/usr/bin/python

import unittest
import legion
import socket
import threading
import StringIO
import simplejson

from legion.jobs import Job

class TestJob(unittest.TestCase):
    def setUp(self):
        self.job_file = StringIO.StringIO()
        self.job_description = {
            'filename': 'legion.blend',
            'startframe': 1,
            'endframe': 250,
            'step': 5,
            'timeout': 180,
            'jobdir': 'jobdir',
            'jobname': 'legionjob',
            'image_x': 800,
            'image_y': 600,
            'xparts': 4,
            'yparts': 4,
        }
        self.job_file.write(simplejson.dumps(self.job_description))
        self.job_file.seek(0)

    def test_load_job(self):
        job = Job(self.job_file)

    def test_invalid_key(self):
        pass


class TestClient(unittest.TestCase):
    def setUp(self):
        pass

class TestClient(unittest.TestCase):
    def setUp(self):
        pass

class TestMaster(unittest.TestCase):
    def XXXtest_ohai(self):
        self.port = 4200
        c = Connect(':%s' % (self.port))
        c.putln('o hai')
        c.expect('o hai 4')

class Connect():
    def __init__(self, info=":"):
        (host, port) = info.split(":")
        port = int(port) or 4200
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.sock.setblocking(0)
        self.sock.connect((host, port))
        self.fh = self.sock.makefile('rw', 0)
        self._cmds = []

    def expect(self, s):
        reply = self.fh.read()
        assert(s == reply, "reply was %s" % (reply))

    def putln(self, s):
        self.put("%s\n" % (s));

    def put(self, s):
        self.fh.write(s)
