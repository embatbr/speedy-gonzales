# -*- coding: utf-8 -*-

import boto3
import csv
from datetime import datetime as dt
from io import StringIO


FORMATTERS = {
    'trim': lambda s: _trim(s),
    'pretty_cpf': lambda cpf: _pretty_cpf(cpf),
    'pretty_pis': lambda pis: _pretty_pis(pis),
    'to_date': lambda date: _to_date(date),
    'timestamp_to_date': lambda timestamp: _timestamp_to_date(timestamp),
    'ensure_int': lambda int_candidate: _ensure_int(int_candidate)
}

def _trim(s):
    if s is None:
        return None

    trimmed = s.strip()
    if not trimmed:
        return None

    return trimmed

def _pretty_cpf(cpf):
    trimmed = _trim(cpf)
    if not trimmed:
        return None

    return '{}.{}.{}-{}'.format(trimmed[:3], trimmed[3:6], trimmed[6:9], trimmed[9:])

def _pretty_pis(pis):
    trimmed = _trim(pis)
    if not trimmed:
        return None

    return '{}.{}.{}-{}'.format(trimmed[:3], trimmed[3:8], trimmed[8:10], trimmed[10])

def _to_date(date_str):
    trimmed = _trim(date_str)
    if not trimmed:
        return None

    try:
        date = dt.strptime(trimmed, '%Y-%m-%d')
        return date.strftime('%Y-%m-%d')
    except Exception:
        return None

def _timestamp_to_date(timestamp):
    trimmed = _trim(timestamp)
    if not trimmed:
        return None

    try:
        trimmed = trimmed.split('T')[0]
        date = dt.strptime(trimmed, '%Y-%m-%d')
        return date.strftime('%Y-%m-%d')
    except Exception:
        return None

def _ensure_int(int_candidate):
    if isinstance(int_candidate, int):
        return int_candidate

    if not isinstance(int_candidate, str):
        return None

    if not int_candidate.isdigit():
        return None

    return int(int_candidate)


def transform(memory, func, err_class, filter_by_success=True):
    def __transform(obj):
        try:
            return (True, func(obj))
        except err_class:
            return (False, obj)

    filter_successes = lambda tup: tup[0]
    filter_failures = lambda tup: not tup[0]
    filter_func = filter_successes if filter_by_success else filter_failures

    extract = lambda tup: tup[1]

    memory['rdd'] = memory['rdd'].map(__transform)
    memory['rdd'] = memory['rdd'].filter(filter_func)
    memory['rdd'] = memory['rdd'].map(extract)


def deep_get(obj, key_seq):
    ret = None

    for key in key_seq:
        if not isinstance(obj, dict):
            return None

        if key not in obj.keys():
            return None

        ret = obj = obj[key]

    return ret


def seq_to_csv(delimiter):
    def _internal(seq):
        row = StringIO()
        writer = csv.writer(row, delimiter=delimiter, lineterminator='')
        writer.writerow(seq)

        return row.getvalue()

    return _internal


def upload_to_s3(bucket_name, key, data):
    s3_resource = boto3.resource('s3')
    obj = s3_resource.Object(bucket_name, key)
    obj.put(Body=data)
