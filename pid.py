import logging
import os
import signal


class Pid:
    _logger = logging.getLogger()

    @classmethod
    def resolve_pid(cls, pidfile_path):

        if os.path.isfile(pidfile_path):
            pidfile = open(pidfile_path, 'r')
            other_pid = pidfile.read()
            pidfile.close()

            cls._logger.info("Previous process pid: ", str(other_pid))
            try:
                os.kill(int(other_pid), signal.SIGINT)
            except:
                cls._logger.info("previous process doesn't exist")

        my_pid = str(os.getpid())
        pidfile = open(pidfile_path, 'w')
        pidfile.write(my_pid)
        pidfile.close()
