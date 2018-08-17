# -*- coding: utf-8 -*-


import json
import os
import requests as r
import time


PROJECT_ROOT_PATH = os.environ.get('PROJECT_ROOT_PATH')

AWS_ACCESS_KEY_ID = os.environ.get('AWS_ACCESS_KEY_ID')
AWS_SECRET_ACCESS_KEY = os.environ.get('AWS_SECRET_ACCESS_KEY')

TEXT_FILEPATH = os.environ.get('TEXT_FILEPATH', '{}/README.md'.format(PROJECT_ROOT_PATH))


def start_or_stop(command):
    resp = r.post(
        'http://localhost:8000/spark',
        data=json.dumps({
            'command': command
        })
    )
    print(resp)


def get_status():
    resp = r.get('http://localhost:8000/spark/status')
    print(resp)
    return resp.json()['online']


def submit():
    payload = {
        'steps': [
            ['load_rdd', TEXT_FILEPATH],
            ['collect']
        ]
    }

    if AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY:
        payload['options'] = {
            's3': {
                'aws_access_key_id': AWS_ACCESS_KEY_ID,
                'aws_secret_access_key': AWS_SECRET_ACCESS_KEY
            }
        }

    resp = r.post(
        'http://localhost:8000/jobs/submit',
        data=json.dumps(payload)
    )
    print(resp)
    return resp.json()['job_id']


def get_job(job_id):
    resp = r.get('http://localhost:8000/jobs/{}'.format(job_id))
    print(resp)
    return resp.json()['status']


if __name__ == '__main__':
    start_or_stop('start')

    online = False
    while not online:
        time.sleep(5)
        online = get_status()

    print('Spark is online')
    print('Submitting job')
    job_id = submit()

    if job_id:
        print(job_id)
        status = None
        while status != 'FINISHED':
            time.sleep(1)
            status = get_job(job_id)
            print(status)
    else:
        print('Job submission failed')

    time.sleep(5)
    start_or_stop('stop')
