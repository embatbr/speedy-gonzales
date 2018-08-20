# -*- coding: utf-8 -*-


import falcon
import json


class JobController(object):

    def __init__(self, spark_executor):
        self.spark_executor = spark_executor

    def on_post(self, req, resp):
        if req.path != '/jobs/submit':
            resp.status_code = falcon.HTTP_400
            return

        payload = req.stream.read()
        try:
            payload = payload.decode('utf8')
            payload = json.loads(payload)
        except json.decoder.JSONDecodeError:
            resp.status_code = falcon.HTTP_400

        options = payload.get('options')
        steps = payload.get('steps')

        job_id = self.spark_executor.submit_job(options, steps)
        if not job_id:
            resp.status_code = falcon.HTTP_422
            return

        resp.status_code = falcon.HTTP_200
        resp.body = json.dumps({
            'job_id': job_id
        })

    def on_get(self, req, resp, action, job_id=None):
        if action == 'pop' and job_id is None:
            resp.status_code = falcon.HTTP_200
            resp.body = json.dumps(self.spark_executor.pop_job())

        elif action == 'status':
            resp.status_code = falcon.HTTP_200
            resp.body = json.dumps({
                'status': self.spark_executor.get_job_status(job_id)
            })
