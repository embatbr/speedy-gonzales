# -*- coding: utf-8 -*-


import json


def load_rdd(memory, filepath):
    memory['rdd'] = memory['spark_context'].textFile(filepath)


def jsonify(memory):
    memory['rdd'] = memory['rdd'].map(lambda s: json.loads(s))


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
