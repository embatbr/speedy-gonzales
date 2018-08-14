# -*- coding: utf-8 -*-


import os


class SparkExecutor(object):

    def execute(self):
        PROJECT_ROOT_PATH = os.environ.get('PROJECT_ROOT_PATH')

        os.system('{}/spark/run.sh'.format(PROJECT_ROOT_PATH))
