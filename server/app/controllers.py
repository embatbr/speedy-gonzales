# -*- coding: utf-8 -*-


import falcon
import json


class IndexController(object):

    def on_get(self, req, resp):
        resp.status_code = falcon.HTTP_200
        resp.body = 'Ariba! Ariba! Andale! Andale!'


class SparkController(object):

    def __init__(self, spark_executor):
        self.spark_executor = spark_executor

    def on_get(self, req, resp, action):
        if action not in {'start', 'stop', 'status'}:
            resp.status_code = falcon.HTTP_400
            return

        if action in {'start', 'stop'}:
            action_method = getattr(self.spark_executor, action)
            action_method()

        resp.status_code = falcon.HTTP_200
        resp.body = json.dumps({
            'status': 'ONLINE' if self.spark_executor.is_online() else 'OFFLINE'
        })


class SubmitJobController(object):

    def __init__(self, spark_executor):
        self.spark_executor = spark_executor

    def on_post(self, req, resp):
        if req.path != '/jobs/submit':
            resp.status_code = falcon.HTTP_400
            return

        payload = req.stream.read()
        payload = payload.decode('utf8')
        payload = json.loads(payload)

        job_id = self.spark_executor.submit_job(payload)
        if not job_id:
            resp.status_code = falcon.HTTP_422
            return

        resp.status_code = falcon.HTTP_200
        resp.body = json.dumps({
            'job_id': job_id
        })

    def on_get(self, req, resp, job_id):
        resp.status_code = falcon.HTTP_200
        resp.body = json.dumps({
            'status': self.spark_executor.get_job_status(job_id)
        })
