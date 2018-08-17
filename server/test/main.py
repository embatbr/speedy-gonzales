# -*- coding: utf-8 -*-


import json
import os
import requests as r


PROJECT_ROOT_PATH = os.environ.get('PROJECT_ROOT_PATH')


def start_or_stop(command):
    resp = r.post(
        'http://localhost:8000/spark',
        data=json.dumps({
            'command': command
        })
    )

    print(resp)


def submit():
    resp = r.post(
        'http://localhost:8000/jobs/submit',
        data=json.dumps({
            'options': {
                's3': {
                    'aws_access_key_id': os.environ.get('AWS_ACCESS_KEY_ID'),
                    'aws_secret_access_key': os.environ.get('AWS_SECRET_ACCESS_KEY')
                }
            },
            'steps': [
                ['load_rdd', os.environ.get('S3_TEST_KEY')],
                ['jsonify'],
                ['take', 1]
            ]
        })
    )

    print(resp)


if __name__ == '__main__':
    running = True

    while running:
        command = input('command (start|stop|submit|exit): ').strip()
        if command in ['start', 'stop']:
            start_or_stop(command)
        elif command == 'submit':
            submit()
        elif command == 'exit':
            running = False
            print('Exiting...')
        else:
            print("Unknown command '{}'".format(command))
