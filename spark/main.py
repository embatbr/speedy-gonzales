# -*- coding: utf-8 -*-


import json
import os
from pyspark.sql import SparkSession
import sys

from sequencers import SequenceBuilder
import settings


def execute(steps):
    spark_context = SparkSession.builder.appName('speedy-gonzales').getOrCreate().sparkContext
    spark_context._conf.set("spark.executorEnv.JAVA_HOME", settings.JAVA_HOME)
    spark_context._conf.set("spark.driver.maxResultSize", settings.MAX_RESULT_SIZE)

    sequence_builder = SequenceBuilder(spark_context)
    sequence_executor = sequence_builder.build(steps)

    sequence_executor.execute()
    print(sequence_executor.memory['rdd'].collect()) # for debug only

    spark_context.stop()


def read_socket(socket):
    filenames = os.listdir(socket)
    if filenames:
        filenames = sorted(filenames)
        filepath = '{}/{}'.format(socket, filenames[0])

        with open(filepath) as file:
            return (json.load(file), filepath)

    return None


if __name__ == '__main__':
    import time

    options = sys.argv[1]
    options = json.loads(options)

    HOME = os.environ.get('HOME')
    socket = '{}/socket'.format(HOME)

    while True:
        ret = read_socket(socket)

        if ret:
            (data, filepath) = ret
            steps = data.get('steps')
            execute(steps)
            os.remove(filepath)
        else:
            print('Sleeping for {} seconds'.format(settings.HEARTBEAT))
            time.sleep(settings.HEARTBEAT)
