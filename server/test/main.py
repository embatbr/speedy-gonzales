# -*- coding: utf-8 -*-


import json
import os
import requests as r
import time


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
            'steps': [
                ['load_rdd', '{}/README.md'.format(PROJECT_ROOT_PATH)],
                ['filter_empty_lines'],
                ['remove_spaces']
            ]
        })
    )

    print(resp)
    resp = r.post(
        'http://localhost:8000/jobs/submit',
        data=json.dumps({
            'steps': [
                ['load_rdd', '{}/spark/main.py'.format(PROJECT_ROOT_PATH)],
                ['remove_spaces']
            ]
        })
    )

    print(resp)


if __name__ == '__main__':
    while True:
        command = input('command (start|stop|submit): ').strip()
        if command in ['start', 'stop']:
            start_or_stop(command)
        elif command == 'submit':
            submit()
        else:
            print("Unknown command '{}'".format(command))
