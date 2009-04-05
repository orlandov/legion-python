#!python

import types
import simplejson
import time

from legion.log import log 
from legion.error import LegionError

# XXX this is horribly written, refactor this asap!
class Job(object):
    def __init__(self, job_file):
        # read in the job file and assign the keys to ourself
        valid_keys = [
            'filename', 'startframe', 'endframe', 'step', 'timeout', 'jobdir',
            'jobname', 'image_x', 'image_y', 'xparts', 'yparts'
        ]

        if type(job_file) == types.StringType:
            fh = file(job_file, 'r')
        else:
            fh = job_file 

        job_data = simplejson.loads(fh.read())

        for key in job_data:
            if key not in valid_keys: 
                raise LegionError('Invalid key in job file, "%s"' % key)
            setattr(self, "%s" % (key), job_data[key])

        self.id = time.time()
        self.job_file = job_file
        self.tasks = []
        self.status = 'pending'
        self.frames()

    def frames(self):
        frame = self.startframe
        self.type = 'frames'

        self.tasks.extend([
            dict(
                number    = i,
                frame     = frame+i,
                status    = 'pending',
                allocated = 0
            )
            for i in range(self.startframe-1, self.endframe)
        ])

    def parts(self):
        # unimplemented
        pass

    def cleanup(self):
        os.system('rm -R "%s"' % (self._job_dir))

    def next_step_for(self, assigned_to):
        log.msg("get next step for job %d" % (self._id))
        for task in job.tasks:
            if task.status not in ['pending', 'error']:
                continue
            
            task.status = 'rendering'
            task.assigned_to = assigned_to
            task.start_time = time.time()

            if job.type == 'frame':
                start = task.frame
                end = task.frame + job.step
                return (start, end)
            elif job.type == 'parts':
                pass

    def set_task_status(self, frame, status, t):
        log.msg("Job %d: Setting task status for frame %d to '%s'"
            % (self.id, status))

        for task in self.tasks:
            if task.frame != frame: continue

            if job.frame == frame:
                task.status = status
                if t: task.rendertime = t
                break

# XXX this could/should be turned into an iterator
class Jobs(object):
    def __init__(self):
        self._jobs = {}

    def all(self):
        return self._jobs

    def by_status(self, status):
        return (job for job in self.all() if job.status == status)

    def active(self):
        return self.by_status('active')

    def pending(self):
        return self.by_status('pending')

    # equiv to fj new_job
    def add_job(job):
        self._jobs[job._id] = job

    def delete_job(self, client, job_id):
        self._jobs[job.id].cleanup()
        del self._jobs[job.id]

    def active_job(self):
        self.check_finished_jobs();
        log.msg("getting active job")

        for job in self.active():
            log.msg("job %d" % (job.id(),));
            log.msg("status %d" % (job.status(),));
            
            # check for pending or error tasks
            for task in job.tasks():
                if task.status in ['pending', 'error']:
                    return job
       
        # if there were no active jobs, make the first pending active
        for job in self.pending():
            job.status = 'active'
            return job

        # no jobs
        return None

    def check_finished_jobs(self):
        log.msg("checking status of all jobs")
        for job in self._jobs:
            running = None
            for task in job.tasks:
                if task.status != 'completed':
                    running = True

            if not running:
                job.status = 'completed'

                # parts are unimplemented
                if job.type == 'parts':
                    pass

            log.msg("job %d status = %s" % (job.status))
    
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
