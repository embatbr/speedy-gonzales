# -*- coding: utf-8 -*-


def load_rdd(memory, filepath):
    spark_context = memory['spark_context']
    rdd = spark_context.textFile(filepath)

    memory['rdd'] = rdd


def filter_empty_lines(memory):
    rdd = memory['rdd']

    rdd = rdd.filter(lambda s: s.strip())

    memory['rdd'] = rdd


def remove_spaces(memory):
    rdd = memory['rdd']

    rdd = rdd.map(lambda s: s.replace(' ', ''))

    memory['rdd'] = rdd