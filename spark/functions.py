# -*- coding: utf-8 -*-


import json


def load_rdd(memory, filepath):
    spark_context = memory['spark_context']
    rdd = spark_context.textFile(filepath)

    memory['rdd'] = rdd


def jsonify(memory):
    rdd = memory['rdd']

    rdd = rdd.map(lambda s: json.loads(s))

    memory['rdd'] = rdd


def take(memory, amount):
    rdd = memory['rdd']

    dataset = rdd.take(amount)

    memory['dataset'] = dataset


def collect(memory):
    rdd = memory['rdd']

    dataset = rdd.collect()

    memory['dataset'] = dataset


def split_into_tables(memory, input_fields_by_table):
    def __split(obj):
        splitted = dict()

        for (table, input_fields) in input_fields_by_table.items():
            splitted[table] = dict()
            for input_field in input_fields:
                splitted[table][input_field] = obj.get(input_field)

        return splitted

    rdd = memory['rdd']

    rdd = rdd.map(__split)

    memory['rdd'] = rdd
