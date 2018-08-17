# -*- coding: utf-8 -*-


import json
import os
import shutil
import time


class SparkExecutor(object):

    def __init__(self):
        HOME = os.environ.get('HOME')
        self.queue = '{}/queue'.format(HOME)

    def start(self):
        self.stop()
        os.makedirs(self.queue)

        PROJECT_ROOT_PATH = os.environ.get('PROJECT_ROOT_PATH')

        command = "{}/spark/run.sh &".format(PROJECT_ROOT_PATH)

        os.system(command)

    def stop(self):
        os.system("kill -9 $(ps aux | grep spark | grep -v grep | awk {'print$2'})")

        if os.path.exists(self.queue):
            shutil.rmtree(self.queue)

    def submit(self, payload):
        filepath = '{}/{}.json'.format(self.queue, time.time())
        with open(filepath, 'w') as file:
            json.dump(payload, file, indent=4, ensure_ascii=False)
