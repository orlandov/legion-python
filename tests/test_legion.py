#!/usr/bin/python

import StringIO
import copy
import simplejson
import sys
import unittest

sys.path.append('lib')

from legion.jobs import Job, Jobs, Task
from legion.master import Master
from legion.error import LegionError

class Mocked(object):
    def __init__(self, *args, **kwargs):
        for k in kwargs:
            setattr(self, k, kwargs[k])


class MockClient(Mocked):
    def __init__(self, *args, **kwargs):
        self.received = []
        self.id = 0
        self.status = 'idle'
        Mocked.__init__(self, *args, **kwargs)

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
            'endframe': 6,
            'tasksize': 2,
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
        self.job_file.seek(0)
        self.job_file.write(simplejson.dumps(self.job_dict))
        self.job_file.seek(0)

    def test_load_job(self):
        job = Job(self.job_file)

        self.assertEqual(job.type, 'frame')
        self.assertEqual(job.status, 'pending')
        self.assertEqual(job.tasks, [
            Task(
                startframe=x,
                endframe=x+1,
                status='pending',
                allocated=0
            ) for x in range(1, 7, 2) ]
        )

    def test_invalid_key(self):
        self.job_dict['invalid'] = 'invalid key'
        self.update_job_file()

        def load_job():
            Job(self.job_file)

        self.assertRaises(LegionError, load_job)

    def test_next_step(self):
        c = MockClient()
        job = Job(self.job_file)

        task = job.assign_next_task(c)
        self.assertEqual(task.startframe, 1, 'start frame is correct')
        self.assertEqual(task.endframe, 2, 'end frame is correct')
        self.assertEqual(task.client, c, 'client is correct') 
        task.status = 'complete'

        task = job.assign_next_task(c)
        self.assertEqual(task.startframe, 3, 'start frame is correct')
        self.assertEqual(task.endframe, 4, 'end frame is correct')
        task.status = 'complete'

        task = job.assign_next_task(c)
        self.assertEqual(task.startframe, 5, 'start frame is correct')
        self.assertEqual(task.endframe, 6, 'end frame is correct')
        task.status = 'complete'

        task = job.assign_next_task(c)

        self.assertEqual(task, None, 'None returned when all tasks complete')

        self.job_dict['tasksize'] = 5
        self.update_job_file()
        job = Job(self.job_file)

        task = job.assign_next_task(c)
        self.assertEqual(task.startframe, 1, 'start frame is correct')
        self.assertEqual(task.endframe, 5, 'end frame is correct')
        task.status = 'complete'

        task = job.assign_next_task(c)
        self.assertEqual(task.startframe, 6, 'start frame is correct')
        self.assertEqual(task.endframe, 6, 'end frame is correct')

        self.job_dict['tasksize'] = 1
        self.job_dict['startframe'] = 1
        self.job_dict['endframe'] = 1
        self.update_job_file()
        job = Job(self.job_file)

        task = job.assign_next_task(c)
        self.assertEqual(task.startframe, 1, 'start frame is correct')
        self.assertEqual(task.endframe, 1, 'end frame is correct')

        self.job_dict['tasksize'] = 3
        self.job_dict['startframe'] = 1
        self.job_dict['endframe'] = 1
        self.update_job_file()
        job = Job(self.job_file)

        task = job.assign_next_task(c)
        self.assertEqual(task.startframe, 1, 'start frame is correct')
        self.assertEqual(task.endframe, 1, 'end frame is correct')

    def test_set_task_status(self):
        c = MockClient()
        job = Job(self.job_file)

        task = job.assign_next_task(c)
        self.assertEqual(task.startframe, 1, 'start frame is correct')
        self.assertEqual(task.endframe, 2, 'end frame is correct')
        job.set_task_status(1, 'complete', 10)

        task = job.assign_next_task(c)
        self.assertEqual(task.startframe, 3, 'start frame is correct')
        self.assertEqual(task.endframe, 4, 'end frame is correct')
        self.assertEqual(job.tasks[0].status, 'complete')
        self.assertEqual(job.tasks[1].status, 'rendering')

class MockJob(Mocked):
    def __init__(self, *args, **kwargs):
        self.all_tasks_complete = False
        Mocked.__init__(self, *args, **kwargs)

    def cleanup(self):
        pass

    def all_tasks_complete(self):
        return self.all_tasks_complete

class TestJobs(unittest.TestCase):
    def setUp(self):
        self.jobobjs = [
            MockJob(id=1, status='complete'),
            MockJob(id=2, status='pending'),
            MockJob(id=3, status='pending')
        ]

        self.jobs = Jobs()

    def add_jobs(self):
        for job in self.jobobjs:
            self.jobs.add_job(job)

    def test_add_job(self):
        self.add_jobs()

        self.assertEqual(
            [ job for job in self.jobs.all() ], 
            [1, 2, 3]
        )
        self.assertEqual(
            [ job.id for job in self.jobs.pending()  ], 
            [2, 3]
        )

    def test_delete_job(self):
        self.add_jobs()

        self.jobs.delete_job(1)
        self.assertEqual(
            [ job for job in self.jobs.all()  ], 
            [2, 3]
        )

    def test_active_job(self):
        self.add_jobs()
        active = self.jobs.active_job()
        self.assertEqual(active.id, 2)

        jobs = Jobs()
        active = jobs.active_job()
        self.assertEqual(active, None, 'no jobs available')

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
