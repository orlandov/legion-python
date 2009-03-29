#!python

from legion.socketstream import SocketStream
from legion.log import log

class Client(object):
    def __init__(self, conn, status='idle'):
        self._status = status
        self.conn = conn
        self.socketstream = SocketStream(conn)
        self.type = 'slave'

    def status(self, status):
        if status not in ['idle', 'busy']:
            raise Exception('Invalid status, "%s"' % (status))
        self._status = status

    def is_idle(self):
        return self.status == 'idle'

    def is_busy(self):
        return self.status == 'busy'

    def write(self, s):
        log.debug(">>> %d | %s" % (self.conn.fileno(), s))
        self.socketstream.write(s)
        self.socketstream.finish()

    def start_job(self, job, cmd):
        cmdstr = 'CMD %s RENDER -S %s -E %s -A "%s|%s"' % (
            job.id, cmd.start, cmd.end, job.jobdir, job.blendfile)
        self.write(cmdstr)
        self.status('busy')
