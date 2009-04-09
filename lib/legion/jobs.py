#!python

import types
import simplejson
import time

from legion.log import log 
from legion.error import LegionError

class Task(object):
    KEYS = "startframe endframe status client".split()
    def __init__(self, **kwargs):
        self.client = None
        for k in kwargs:
            if k not in Task.KEYS: raise LegionError("Invalid key %s" % (k,))
            setattr(self, k, kwargs[k])

    def __eq__(self, other):
        return self.__dict__ == other.__dict__

    def to_hash(self):
        hash = {}
        for k in Task.KEYS:
            hash[k] = getattr(self, k)
        return hash

class Job(object):

    KEYS="id filename startframe endframe tasksize timeout jobdir jobname".split()

    def __init__(self, job_file):
        if type(job_file) == types.StringType:
            fh = file(job_file, 'r')
        else:
            fh = job_file 

        job_data = simplejson.loads(fh.read())

        for key in job_data:
            if key not in Job.KEYS: 
                raise LegionError('Invalid key in job file, "%s"' % key)
            setattr(self, "%s" % (key), job_data[key])

        self.id = int(time.time())
        self.job_file = job_file
        self.tasks = []
        self.status = 'pending'
        self.frames()

    def to_hash(self):
        hash = {}
        for key in Job.KEYS:
            hash[key] = getattr(self, key)
        return hash

    def frames(self):
        frame = self.startframe
        self.type = 'frame'

        self.tasks.extend([
            Task(
                startframe=frame,
                endframe=
                   (frame+self.tasksize-1)
                   if frame+self.tasksize-1 < self.endframe
                   else frame+((self.endframe-self.startframe) % (self.tasksize)),
                status='pending'
            )
            for frame in range(self.startframe, self.endframe + 1, self.tasksize)
        ])

    def all_tasks_complete(self):
        complete = [] == [ 1 for task in self.tasks if task.status != 'complete' ]
        log.msg("%s" % (complete,))
        if complete:
            self.status = 'complete'
            log.msg("All tasks complete for job %d" % (self.id))
        return complete

    def cleanup(self):
        os.system('rm -R "%s"' % (self.job_dir))

    def assign_next_task(self, client):
        log.msg("get next step for job %d" % (self.id))
        start = end = None

        incomplete = (
            task
            for task in self.tasks
            if task.status in ['pending', 'error']
        )
       
        try:
            next_task = incomplete.next()
            next_task.status = 'rendering'
            next_task.client = client
            next_task.start_time = time.time()
            return next_task
        except StopIteration:
            return None

    def set_task_status(self, frame, status):
        log.msg("Job %d: Setting task status for frame %d to '%s'"
            % (self.id, frame, status))

        for task in self.tasks:
            if task.startframe != frame: continue
            task.status = status

# XXX this could/should be turned into an iterator
class Jobs(object):
    def __init__(self):
        self.jobs = {}
        self.job_ids = []
        self.active_job_id = None

    def all(self):
        return self.jobs

    def by_status(self, status):
        return (self.jobs[job_id]
                for job_id in self.job_ids
                if self.jobs[job_id].status == status)

    def pending(self):
        return self.by_status('pending')

    def add_job(self, job):
        self.job_ids.append(job.id)
        self.jobs[job.id] = job

    def get_job(self, id):
        return self.jobs[id]

    def delete_job(self, job_id):
        self.jobs[job_id].cleanup()
        del self.job_ids[self.job_ids.index(job_id)]
        del self.jobs[job_id]

    def active_job(self):
        log.msg("getting active job")


        if not self.active_job_id or \
            self.jobs[self.active_job_id].status == 'complete' or \
            self.jobs[self.active_job_id].all_tasks_complete():
            try:
                self.active_job_id = self.pending().next().id
            except StopIteration:
                return None # no jobs

        return self.jobs[self.active_job_id]


    def check_timed_out_tasks(self):
        log.msg("checking for timed out tasks")
        for job in self.active:
            for task in job.tasks:
                if not (task.status == 'rendering'
                    and task.start_time < time - job.timeout): break

                job.status = 'pending'
                log.warn("job %d frame %d has timed out and been re-queued")


    def reset_tasks(self, job_id=None, slave=None):
        all_complete = True
        for job in self.all():
            if (job_id not in [job.id, None]
               and job.status != 'complete'
               and job.status != 'paused'): continue

            for task in job.tasks:
                if task.assigned_to != slave:
                    continue

                if task.status not in ['completed', 'pending']:
                    task.status = 'pending'
                    task.assigned_to = None
                if task.status != 'complete':
                    all_complete = False
            
            if not all_complete:
                log.msg("setting job %d status to pending" % (job.id,))
                job.status = 'pending'

            if job_id: break
