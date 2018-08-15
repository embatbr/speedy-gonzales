# -*- coding: utf-8 -*-


def load_rdd(memory, filepath):
    spark_context = memory['spark_context']
    return spark_context.textFile(filepath)
