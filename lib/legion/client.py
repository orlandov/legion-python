#!python

from legion.log import log

class Client(object):
    __next_id = 0

    def __init__(self, protocol, status='idle'):
        self._status = status
        self._type = 'slave'
        self._protocol = protocol
        self._id = self.new_id()

    @property
    def id(self):
        return self._id
    
    def new_id(self):
        id = Client.__next_id
        Client.__next_id += 1
        return id

    def get_status(self):
        return self._status

    def set_status(self, status):
        if status not in ['idle', 'busy']:
            raise Exception('Invalid status, "%s"' % (status))
        self._status = status

    status = property(get_status, set_status)

    def is_idle(self):
        return self.status == 'idle'

    def is_busy(self):
        return self.status == 'busy'

    def send_line(self, s):
        log.msg(">>> %d | %s" % (self.id, s))
        self._protocol.sendLine(s)

    def start_job(self, job, cmd):
        cmdstr = 'CMD %s RENDER -S %s -E %s -A "%s|%s"' % (
            job.id, cmd.start, cmd.end, job.jobdir, job.blendfile)
        self.send_line(cmdstr)
        self.status = 'busy'
