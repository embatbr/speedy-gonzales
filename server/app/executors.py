# -*- coding: utf-8 -*-


import json
import os


class SparkExecutor(object):

    def execute(self, params):
        PROJECT_ROOT_PATH = os.environ.get('PROJECT_ROOT_PATH')

        command = "{}/spark/run.sh '{}'".format(
            PROJECT_ROOT_PATH,
            json.dumps(params, ensure_ascii=False)
        )

        os.system(command)
