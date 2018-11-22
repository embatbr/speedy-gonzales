# -*- coding: utf-8 -*-


import json

from functions_utils import transform, deep_get, seq_to_csv, upload_to_s3, FORMATTERS


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


def split_into_tables(memory, fields_by_table):
    def _split(obj):
        splitted = dict()

        for (table, fields_and_extractors) in fields_by_table.items():
            splitted[table] = dict()

            fields = fields_and_extractors['fields']
            for field in fields:
                sub_obj = obj.get(field)
                splitted[table][field] = sub_obj

        return splitted

    memory['rdd'] = memory['rdd'].map(_split)


def group_by_table(memory, tables):
    memory['tables'] = dict()

    def _group(table):
        def __internal(obj):
            return obj[table]
        return __internal

    for table in tables:
        memory['tables'][table] = memory['rdd'].map(_group(table))


def flatten_tables(memory, extractors_by_table):
    def _flatten(fields_and_extractors):
        def __internal(obj):
            for (field, extractors) in fields_and_extractors.items():
                for (sub_key, key_seq) in extractors.items():
                    ret = deep_get(obj[field], key_seq)
                    if ret is not None:
                        obj[sub_key] = ret

                del obj[field]

            return obj

        return __internal

    for (table, table_extractors) in extractors_by_table.items():
        rdd = memory['tables'][table]
        rdd = rdd.map(_flatten(table_extractors))
        memory['tables'][table] = rdd


def explode_tables(memory, fields_by_table):
    def _invert(field):
        def __internal(obj):
            collection_obj = obj.get(field)
            if collection_obj:
                del obj[field]
                new_collection_obj = list()

                for sub_obj in collection_obj:
                    sub_obj.update(obj)
                    new_collection_obj.append(sub_obj)

                return new_collection_obj

            return [obj]

        return __internal

    for (table, fields) in fields_by_table.items():
        for field in fields:
            memory['tables'][table] = memory['tables'][table].map(_invert(field))
            memory['tables'][table] = memory['tables'][table].flatMap(lambda xs: [x for x in xs])


def rename_tables_fields(memory, renamings_by_table):
    def _rename(renamings):
        def __internal(obj):
            keys = obj.keys()
            for (old_field, new_field) in renamings.items():
                if old_field in keys:
                    obj[new_field] = obj[old_field]
                    del obj[old_field]

            return obj

        return __internal

    for (table, renamings) in renamings_by_table.items():
        memory['tables'][table] = memory['tables'][table].map(_rename(renamings))


def expand_tables(memory, expansions_by_table):
    def _expand(expansions):
        def __internal(obj):
            for expansion in expansions:
                field = expansion.get('field')
                default_value = expansion.get('default_value')

                if field is not None:
                    obj[field] = default_value

            return obj

        return __internal

    for (table, expansions) in expansions_by_table.items():
        memory['tables'][table] = memory['tables'][table].map(_expand(expansions))


def format_tables(memory, functions_by_table):
    def _format(functions_by_field):
        def __internal(obj):
            keys = obj.keys()
            for (field, function_name) in functions_by_field.items():
                if field in keys:
                    obj[field] = FORMATTERS[function_name](obj[field])

            return obj

        return __internal

    for (table, functions_by_field) in functions_by_table.items():
        memory['tables'][table] = memory['tables'][table].map(_format(functions_by_field))


def ensure_not_nulls(memory, fields_by_table):
    def _ensure(fields):
        def __internal(obj):
            keys = obj.keys()
            for field in fields:
                if field not in keys:
                    return False
            return True

        return __internal

    for (table, fields) in fields_by_table.items():
        memory['tables'][table] = memory['tables'][table].filter(_ensure(fields))


def unify_tables(memory, unions):
    all_sources_to_delete = set()

    for union in unions:
        sources = union['sources']
        sink = union['sink']

        sources_to_delete = set(union['sources_to_delete'])
        all_sources_to_delete = all_sources_to_delete.union(sources_to_delete)

        rdd_sources = [memory['tables'][source] for source in sources]
        memory['tables'][sink] = memory['spark_context'].union(rdd_sources)

    for source in all_sources_to_delete:
        del memory['tables'][source]


def json_to_list_for_tables(memory, fields_by_table):
    def _convert(fields):
        def __internal(obj):
            return [obj.get(field) for field in fields]
        return __internal

    for (table, fields) in fields_by_table.items():
        memory['tables'][table] = memory['tables'][table].map(_convert(fields))


def upload_tables(memory, tables, bucket_name, keypath, extension):
    for table in tables:
        memory['tables'][table] = memory['tables'][table].map(seq_to_csv('|'))
        data = memory['tables'][table].reduce(lambda x, y: '{}\n{}'.format(x, y))

        key = '{}/{}.{}'.format(keypath, table, extension)
        upload_to_s3(bucket_name, key, data)

        key = '{}/_DONE'.format(keypath)
        upload_to_s3(bucket_name, key, data)
