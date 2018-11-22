# -*- coding: utf-8 -*-

import boto3
import csv
from datetime import datetime as dt
from io import StringIO


FORMATTERS = {
    'str': str,
    'trim': lambda s: _trim(s),
    'pretty_sex': lambda cpf: _pretty_sex(cpf),
    'pretty_cnpj': lambda cpf: _pretty_cnpj(cpf),
    'pretty_cpf': lambda cpf: _pretty_cpf(cpf),
    'pretty_pis': lambda pis: _pretty_pis(pis),
    'pretty_brazilian_voting_number': lambda voting_number: _pretty_brazilian_voting_number(voting_number),
    'pretty_brazilian_voting_zone': lambda zone: _pretty_brazilian_voting_zone(zone),
    'pretty_brazilian_voting_section': lambda section: _pretty_brazilian_voting_section(section),
    'pretty_cep': lambda pis: _pretty_cep(pis),
    'pretty_nirf': lambda nirf: _pretty_nirf(nirf),
    'to_date': lambda date: _to_date(date),
    'timestamp_to_date': lambda timestamp: _timestamp_to_date(timestamp),
    'ensure_int': lambda int_candidate: _ensure_int(int_candidate),
    'ensure_float': lambda float_candidate: _ensure_float(float_candidate),
    'ensure_money': lambda money_candidate: _ensure_money(money_candidate),
    'str2int': lambda s: _str2int(s)
}

def _trim(s):
    if s is None:
        return None

    trimmed = s.strip()
    if not trimmed:
        return None

    return trimmed

def _pretty_sex(sex):
    trimmed = _trim(sex)
    if not trimmed:
        return None

    lowered = trimmed.lower()

    return {
        'masculino': 'M',
        'feminino': 'F'
    }.get(lowered)

def _pretty_cnpj(cnpj):
    trimmed = _trim(cnpj)
    if (not trimmed) or (not trimmed.isdigit()):
        return None

    return '{}.{}.{}/{}-{}'.format(trimmed[:2], trimmed[2:5], trimmed[5:8], trimmed[8:12], trimmed[12:])

def _pretty_cpf(cpf):
    trimmed = _trim(cpf)
    if (not trimmed) or (not trimmed.isdigit()):
        return None

    return '{}.{}.{}-{}'.format(trimmed[:3], trimmed[3:6], trimmed[6:9], trimmed[9:])

def _pretty_pis(pis):
    trimmed = _trim(pis)
    if (not trimmed) or (not trimmed.isdigit()):
        return None

    return '{}.{}.{}-{}'.format(trimmed[:3], trimmed[3:8], trimmed[8:10], trimmed[10])

def _pretty_brazilian_voting_number(voting_number):
    trimmed = _trim(voting_number)
    if (not trimmed) or (not trimmed.isdigit()):
        return None

    return '{} {} {} {}'.format(trimmed[:4], trimmed[4:8], trimmed[8:10], trimmed[10:])

def _pretty_brazilian_voting_zone(zone):
    trimmed = _trim(zone)
    if (not trimmed) or (not trimmed.isdigit()):
        return None

    return trimmed.zfill(3)

def _pretty_brazilian_voting_section(section):
    trimmed = _trim(section)
    if (not trimmed) or (not trimmed.isdigit()):
        return None

    return trimmed.zfill(4)

def _pretty_cep(cep):
    trimmed = _trim(cep)
    if (not trimmed) or (not trimmed.isdigit()):
        return None

    return '{}-{}'.format(trimmed[:5], trimmed[5:])

def _pretty_nirf(nirf):
    trimmed = _trim(nirf)
    if (not trimmed) or (not trimmed.isdigit()):
        return None

    return '{}-{}'.format(trimmed[:7], trimmed[7])

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

def _ensure_float(float_candidate):
    if isinstance(float_candidate, float):
        return float_candidate

    if isinstance(float_candidate, int):
        return float(float_candidate)

    if not isinstance(float_candidate, str):
        return None

    try:
        return float(float_candidate)
    except Exception:
        return None

def _ensure_money(money_candidate):
    money = _ensure_float(money_candidate)
    if money is None:
        return None

    return round(money, 2)

def _str2int(s):
    trimmed = _trim(s)
    if (not trimmed) or (not trimmed.isdigit()):
        return None

    return int(trimmed)


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
        if not isinstance(obj, list):
            pass
        elif not isinstance(obj, dict):
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
