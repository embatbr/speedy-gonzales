# -*- coding: utf-8 -*-


import json

from functions_utils import transform, deep_get, seq_to_csv, upload_to_s3


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

        for (table, fields_and_extractors) in input_fields_by_table.items():
            splitted[table] = dict()

            fields = fields_and_extractors['fields']
            extractors = fields_and_extractors['extractors']
            extractors_keys = extractors.keys()

            for field in fields:
                sub_obj = obj.get(field)

                if field in extractors_keys:
                    extractor = extractors[field]
                    for (sub_key, key_seq) in extractor.items():
                        splitted[table][sub_key] = deep_get(obj[field], key_seq)

                else:
                    splitted[table][field] = sub_obj

        return splitted

    memory['rdd'] = memory['rdd'].map(__split)


def group_by_table(memory, tables):
    memory['tables'] = dict()

    def _group(table):
        def __internal(obj):
            return obj[table]
        return __internal

    for table in tables:
        memory['tables'][table] = memory['rdd'].map(_group(table))


# def extract(memory, extractors):
#     def __extract(obj):
#         extracted_obj = dict()

#         for (table, value) in obj.items():
#             if table in extractors:
#                 fields = value.keys()
#                 fields_to_extract = extractors[table].keys()

#                 extracted_obj[table] = dict()

#                 for field in fields:
#                     if field in fields_to_extract:
#                         key_seq = extractors[table][field]
#                         extracted_obj[table][field] = deep_get(key_seq)(value[field])
#                     else:
#                         extracted_obj[table][field] = value[field]
#             else:
#                 extracted_obj[table] = value

#         return extracted_obj

#     memory['rdd'] = memory['rdd'].map(__extract)


def json_to_list_for_tables(memory, input_fields_by_table):
    def _convert(fields):
        def __internal(obj):
            return [obj.get(field) for field in fields]
        return __internal

    for (table, input_fields) in input_fields_by_table.items():
        memory['tables'][table] = memory['tables'][table].map(_convert(input_fields))


def upload_tables(memory, tables, bucket_name, keypath, extension):
    def _stringify(row):
        return '|'.join([str(r) for r in row])

    for table in tables:
        memory['tables'][table] = memory['tables'][table].map(seq_to_csv('|'))
        data = memory['tables'][table].reduce(lambda x, y: '{}\n{}'.format(x, y))

        key = '{}/{}.{}'.format(keypath, table, extension)
        upload_to_s3(bucket_name, key, data)

        key = '{}/_DONE'.format(keypath)
        upload_to_s3(bucket_name, key, data)
