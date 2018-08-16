# -*- coding: utf-8 -*-


import json
import os
import shutil
import time


class SparkExecutor(object):

    def __init__(self):
        HOME = os.environ.get('HOME')
        self.socket = '{}/socket'.format(HOME)

    def start(self, options):
        self.stop(options)
        os.makedirs(self.socket)

        PROJECT_ROOT_PATH = os.environ.get('PROJECT_ROOT_PATH')

        command = "{}/spark/run.sh '{}' &".format(
            PROJECT_ROOT_PATH,
            json.dumps(options, ensure_ascii=False)
        )

        os.system(command)

    def stop(self, options):
        os.system("kill -9 $(ps aux | grep spark | grep -v grep | awk {'print$2'})")

        if os.path.exists(self.socket):
            shutil.rmtree(self.socket)

    def submit(self, payload):
        filepath = '{}/{}.json'.format(self.socket, time.time())
        with open(filepath, 'w') as file:
            json.dump(payload, file, indent=4, ensure_ascii=False)
