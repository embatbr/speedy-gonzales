# -*- coding: utf-8 -*-


import json

from functions_utils import transform


def load_rdd(memory, filepath):
    memory['rdd'] = memory['spark_context'].textFile(filepath)


def store_rdd(memory, filepath, num_partitions=1):
    memory['rdd'].coalesce(num_partitions).saveAsTextFile(filepath)


def jsonify(memory):
    transform(memory, json.loads, json.decoder.JSONDecodeError)


def take(memory, amount):
    memory['dataset'] = memory['rdd'].take(amount)


def collect(memory):
    memory['dataset'] = memory['rdd'].collect()


def split_into_tables(memory, input_fields_by_table):
    def __split(obj):
        splitted = dict()

        for (table, input_fields) in input_fields_by_table.items():
            splitted[table] = dict()
            for input_field in input_fields:
                splitted[table][input_field] = obj.get(input_field)

        return splitted

    memory['rdd'] = memory['rdd'].map(__split)
