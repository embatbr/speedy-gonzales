# -*- coding: utf-8 -*-


import json
import os
import requests as r


PROJECT_ROOT_PATH = os.environ.get('PROJECT_ROOT_PATH')


resp = r.post(
    'http://localhost:8000/jobs/submit',
    data=json.dumps({
        'executor': {
            'steps': [
                ['load_rdd', '{}/README.md'.format(PROJECT_ROOT_PATH)]
            ]
        }
    })
)

print(resp)
