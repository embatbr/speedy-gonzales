# -*- coding: utf-8 -*-


import time
import uuid


class Job(object):

    def __init__(self, options, steps):
        self._id = uuid.uuid4().hex
        self._created_at = str(time.time())

        self._options = options
        self._steps = steps

    @property
    def id(self):
        return self._id

    @property
    def options(self):
        return self._options

    @property
    def steps(self):
        return self._steps


class SimpleQueue(object):

    def __init__(self, capacity=10):
        self._capacity = capacity
        self._queue = list()
        self._queued_jobs = set()

    @property
    def capacity(self):
        return self._capacity

    def push(self, options, steps):
        if len(self._queue) < self.capacity:
            job = Job(options, steps)
            self._queue.append(job)
            self._queued_jobs.add(job.id)

            return job.id

        return None

    def pop(self):
        if len(self._queue) == 0:
            return None

        ret = {
            'job_id': self._queue[0].id,
            'options': self._queue[0].options,
            'steps': self._queue[0].steps
        }
        self._queued_jobs.remove(self._queue[0].id)
        self._queue = self._queue[1:]

        return ret

    def is_job_queued(self, job_id):
        return job_id in self._queued_jobs

    def flush(self):
        self._queue = list()
        self._queued_jobs = set()
