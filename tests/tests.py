#! coding: utf-8


import json
import logging
logging.basicConfig(level=logging.INFO)
import os
import requests as r
import shutil
import time


logger = logging.getLogger('tests')

PROJECT_ROOT_PATH = os.environ.get('PROJECT_ROOT_PATH')

SLEEP_INTERVAL = 1


def submit_job(base_url, filename):
    data = {
        'options': {
        },
        'steps': [
            ['load_rdd', '{}/{}'.format(PROJECT_ROOT_PATH, filename)],
            ['store_rdd', '/tmp/speedy-gonzales/{}'.format(filename.replace('/', '_'))]
        ]
    }

    logger.info('Submitting job.')
    resp = r.post('{}/jobs/submit'.format(base_url), data=json.dumps(data))

    job_id = resp.json().get('job_id')
    logger.info('job_id: {}'.format(job_id))

    return job_id


def check_job_status(base_url, job_id, filename):
    status = None
    while status != 'NOT_FOUND':
        logger.info('Checking status for job {}'.format(job_id))
        resp = r.get('{}/jobs/status/{}'.format(base_url, job_id))

        status = resp.json().get('status')
        logger.info('status: {}'.format(status))

        time.sleep(SLEEP_INTERVAL)

    with open('/tmp/speedy-gonzales/{}/part-00000'.format(filename)) as f:
        content = f.read()
        print()
        print('job_id: {}'.format(job_id))
        print(content)
        print()


def execute():
    filepath = '/tmp/speedy-gonzales'
    if os.path.exists(filepath):
        shutil.rmtree(filepath)

    base_url = 'http://localhost:8000'

    job_id_1 = submit_job(base_url, 'README.md')
    job_id_2 = submit_job(base_url, 'tests/tests.py')

    check_job_status(base_url, job_id_1, 'README.md')
    check_job_status(base_url, job_id_2, 'tests_tests.py')


if __name__ == '__main__':
    execute()

    os.system('kill -9 $(cat ../spark/spark.pid)')
    os.remove('{}/spark/spark.pid'.format(PROJECT_ROOT_PATH))
