# -*- coding: utf-8 -*-


import falcon
import json


class IndexAction(object):

    def on_get(self, req, resp):
        resp.status_code = falcon.HTTP_200
        resp.body = json.dumps({
            'foo': 'bar'
        }, indent=4, ensure_ascii=False)


class SubmitAction(object):

    def __init__(self, spark_executor):
        self.spark_executor = spark_executor

    def on_post(self, req, resp):
        body = req.stream.read()
        body = body.decode('utf8')
        body = json.loads(body)

        self.spark_executor.execute()
