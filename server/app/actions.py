# -*- coding: utf-8 -*-


import falcon
import json


class IndexAction(object):

    def on_get(self, req, resp):
        resp.status_code = falcon.HTTP_200
        resp.body = 'Ariba! Ariba! Andale! Andale!'


class SparkAction(object):

    def __init__(self, spark_executor):
        self.spark_executor = spark_executor

    def on_get(self, req, resp):
        if req.path != '/spark/status':
            resp.status_code = falcon.HTTP_405
            return

        resp.status_code = falcon.HTTP_200
        resp.body = json.dumps({
            'online': self.spark_executor.status()
        })

    def on_post(self, req, resp):
        if req.path != '/spark':
            resp.status_code = falcon.HTTP_405
            return

        payload = req.stream.read()
        payload = payload.decode('utf8')
        payload = json.loads(payload)

        command = payload.get('command')

        command_method = getattr(self.spark_executor, command)
        command_method()

        resp.status_code = falcon.HTTP_200


class SubmitJobAction(object):

    def __init__(self, spark_executor):
        self.spark_executor = spark_executor

    def on_get(self, req, resp, job_id):
        resp.status_code = falcon.HTTP_200
        resp.body = json.dumps({
            'status': self.spark_executor.get_job_status(job_id)
        })

    def on_post(self, req, resp):
        if req.path != '/jobs/submit':
            resp.status_code = falcon.HTTP_405
            return

        payload = req.stream.read()
        payload = payload.decode('utf8')
        payload = json.loads(payload)

        job_id = self.spark_executor.submit_job(payload)
        if not job_id:
            resp.status_code = falcon.HTTP_409
            return

        resp.status_code = falcon.HTTP_200
        resp.body = json.dumps({
            'job_id': job_id
        })
