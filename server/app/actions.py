# -*- coding: utf-8 -*-


import falcon
import json


class IndexAction(object):

    def on_get(self, req, resp):
        resp.status_code = falcon.HTTP_200
        resp.body = json.dumps({
            'Ariba': 'Andale'
        })


class SparkAction(object):

    def __init__(self, spark_executor):
        self.spark_executor = spark_executor

    def on_post(self, req, resp):
        payload = req.stream.read()
        payload = payload.decode('utf8')
        payload = json.loads(payload)

        command = payload.get('command')
        options = {}

        command_method = getattr(self.spark_executor, command)
        command_method(options)


class SubmitAction(object):

    def __init__(self, spark_executor):
        self.spark_executor = spark_executor

    def on_post(self, req, resp):
        payload = req.stream.read()
        payload = payload.decode('utf8')
        payload = json.loads(payload)

        self.spark_executor.submit(payload)
