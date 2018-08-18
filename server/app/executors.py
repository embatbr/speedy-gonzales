# -*- coding: utf-8 -*-


import json
import os
import shutil
import time


PROJECT_ROOT_PATH = os.environ.get('PROJECT_ROOT_PATH')


class SparkExecutor(object):

    def __init__(self):
        self.queue = '/tmp/queue'

    def start(self):
        if not self.is_online():
            os.makedirs(self.queue)
            command = "{}/spark/run.sh".format(PROJECT_ROOT_PATH)
            os.system(command)

    def stop(self):
        if os.path.exists("{}/spark/spark.pid".format(PROJECT_ROOT_PATH)):
            os.system("kill -9 $(cat {}/spark/spark.pid)".format(PROJECT_ROOT_PATH))
            os.system("rm {}/spark/spark.pid".format(PROJECT_ROOT_PATH))

        if os.path.exists(self.queue):
            shutil.rmtree(self.queue)

    def is_online(self):
        return os.path.exists(self.queue) and os.path.isdir(self.queue)

    def submit_job(self, payload):
        job_id = time.time()

        filepath = '{}/{}.json'.format(self.queue, job_id)
        try:
            with open(filepath, 'w') as file:
                json.dump(payload, file, indent=4, ensure_ascii=False)
        except Exception:
            return None

        return str(job_id)

    def get_job_status(self, job_id):
        if os.path.exists('{}/{}.json'.format(self.queue, job_id)):
            return 'QUEUED'

        if os.path.exists('{}/spark/job_id'.format(PROJECT_ROOT_PATH)):
            with open('{}/spark/job_id'.format(PROJECT_ROOT_PATH)) as f:
                if f.read() == '{}.json'.format(job_id):
                    return 'RUNNING'

        return 'FINISHED'
