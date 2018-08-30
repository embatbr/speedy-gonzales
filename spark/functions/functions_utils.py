# -*- coding: utf-8 -*-


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


def deep_get(key_seq):
    def _internal(obj):
        ret = None

        for key in key_seq:
            if not isinstance(obj, dict):
                return None

            if key not in obj.keys():
                return None

            ret = obj = obj[key]

        return ret

    return _internal
