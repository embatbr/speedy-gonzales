# -*- coding: utf-8 -*-


import json

from functions_utils import transform, deep_get


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


def extract(memory, extractors):
    def __extract(obj):
        extracted_obj = dict()

        for (table, value) in obj.items():
            if table in extractors:
                fields = value.keys()
                fields_to_extract = extractors[table].keys()

                extracted_obj[table] = dict()

                for field in fields:
                    if field in fields_to_extract:
                        key_seq = extractors[table][field]
                        extracted_obj[table][field] = deep_get(key_seq)(value[field])
                    else:
                        extracted_obj[table][field] = value[field]
            else:
                extracted_obj[table] = value

        return extracted_obj

    memory['rdd'] = memory['rdd'].map(__extract)


# def group_by_table(memory, table_keys):
#     pass
