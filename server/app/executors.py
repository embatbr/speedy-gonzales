# -*- coding: utf-8 -*-


import json
import os
import shutil
import time


PROJECT_ROOT_PATH = os.environ.get('PROJECT_ROOT_PATH')


class SparkExecutor(object):

    def __init__(self, queue):
        self.queue = queue
        self.current_job = None

    def start(self):
        if not self.is_online():
            command = "{}/spark/run.sh".format(PROJECT_ROOT_PATH)
            os.system(command)

    def stop(self):
        if self.is_online():
            self.queue.flush()

            os.system("kill -9 $(cat {}/spark/spark.pid)".format(PROJECT_ROOT_PATH))
            os.remove("{}/spark/spark.pid".format(PROJECT_ROOT_PATH))

    def _has_pid_file(self):
        return os.path.exists("{}/spark/spark.pid".format(PROJECT_ROOT_PATH))

    def _has_job_file(self):
        return os.path.exists("{}/spark/job_id".format(PROJECT_ROOT_PATH))

    def is_online(self):
        return self._has_pid_file()

    def submit_job(self, options, steps):
        self.start()
        return self.queue.push(options, steps)

    def pop_job(self):
        if self.is_online():
            return self.queue.pop()
        return None

    def get_job_status(self, job_id):
        if self.queue.is_job_queued(job_id):
            return 'QUEUED'

        if self._has_job_file():
            with open('{}/spark/job_id'.format(PROJECT_ROOT_PATH)) as f:
                if f.read() == job_id:
                    return 'RUNNING'

        return 'FINISHED'
