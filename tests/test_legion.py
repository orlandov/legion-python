#!/usr/bin/python

import StringIO
import copy
import simplejson
import sys
import unittest

sys.path.append('lib')

from legion.jobs import Job
from legion.master import Master
from legion.error import LegionError

class MockClient(object):
    def __init__(self, *args, **kwargs):
        self.received = []
        self.id = 0
        self.status = 'idle'
        for k in kwargs:
            setattr(self, k, kwargs[k])

    def is_idle(self):
        return self.status == 'idle'

    def send_line(self, line):
        self.received.append(line)

class TestMaster(unittest.TestCase):
    def setUp(self):
        self.m = Master()

    def test_add_client(self):
        c = MockClient()
        self.m.add_client(c)
        self.assertEqual(c.received, ['Welcome client 0'],
            'received welcome string from master')
        self.assertEqual(self.m.clients, { 0: c },
            'new client appears in clients list')

    def test_remove_client(self):
        c = MockClient()
        self.m.add_client(c)
        self.m.remove_client(0)

        self.assertEqual(self.m.clients, {},
            'client was removed')
        self.assertRaises(LegionError, self.m.remove_client, 420)

    def test_get_client(self):
        c = MockClient()
        self.m.add_client(c)
        self.m.get_client(0)

        self.assertEqual(self.m.get_client(0), c, 'client was gotten')
        self.assertRaises(LegionError, self.m.get_client, 420)

    def test_idle_clients(self):
        (p, i0, i1, b) = clients = [
            MockClient(id=0, status='paused'),
            MockClient(id=1, status='idle'),
            MockClient(id=3, status='idle'),
            MockClient(id=2, status='busy'),
        ]

        for c in clients:
            self.m.add_client(c)

        idle = self.m.idle_clients()

        self.assert_(i0 in idle)
        self.assert_(i1 in idle)
        self.assert_(p not in idle)
        self.assert_(b not in idle)

    def test_dispatch_idle_clients(self):
        pass


class TestJob(unittest.TestCase):
    def setUp(self):
        self.job_file = StringIO.StringIO()
        self.job_dict = {
            'filename': 'legion.blend',
            'startframe': 1,
            'endframe': 10,
            'step': 2,
            'timeout': 180,
            'jobdir': 'jobdir',
            'jobname': 'legionjob',
            'image_x': 800,
            'image_y': 600,
            'xparts': 4,
            'yparts': 4,
        }

        self.update_job_file()

    def update_job_file(self):
        self.job_file.write(simplejson.dumps(self.job_dict))
        self.job_file.seek(0)

    def test_load_job(self):
        job = Job(self.job_file)

        self.assertEqual(job.type, 'frames')
        self.assertEqual(job.status, 'pending')
        self.assertEqual(job.tasks, [
            {
                'number': x,
                'frame': x+1,
                'status': 'pending',
                'allocated': 0
            }
            for x in range(10)
        ])


    def test_invalid_key(self):
        self.job_dict['invalid'] = 'invalid key'
        self.update_job_file()

        def load_job():
            Job(self.job_file)

        self.assertRaises(LegionError, load_job)


# class TestClient(unittest.TestCase):
#     def setUp(self):
#         pass
# 
# class TestClient(unittest.TestCase):
#     def setUp(self):
#         pass
# 
# class TestMaster(unittest.TestCase):
#     def XXXtest_ohai(self):
#         self.port = 4200
#         c = Connect(':%s' % (self.port))
#         c.putln('o hai')
#         c.expect('o hai 4')
