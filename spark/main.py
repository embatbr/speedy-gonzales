# -*- coding: utf-8 -*-


import json
import os
from pyspark.sql import SparkSession
import sys

from sequencers import BlockSequenceBuilder
import settings


def execute(options, steps):
    spark_context = SparkSession.builder.appName('speedy-gonzales').getOrCreate().sparkContext
    spark_context._conf.set("spark.executorEnv.JAVA_HOME", settings.JAVA_HOME)
    spark_context._conf.set("spark.driver.maxResultSize", settings.MAX_RESULT_SIZE)
    if options:
        spark_context._jsc.hadoopConfiguration().set("fs.s3n.impl", "org.apache.hadoop.fs.s3native.NativeS3FileSystem")
        if 's3' in options:
            spark_context._jsc.hadoopConfiguration().set("fs.s3n.awsAccessKeyId", options['s3']['aws_access_key_id'])
            spark_context._jsc.hadoopConfiguration().set("fs.s3n.awsSecretAccessKey", options['s3']['aws_secret_access_key'])

    block_sequence_builder = BlockSequenceBuilder(spark_context)
    block_sequence_executor = block_sequence_builder.build(steps)

    block_sequence_executor.execute()
    print('\n'.join(map(lambda x: json.dumps(x), block_sequence_executor.memory['dataset']))) # for debug only

    spark_context.stop()


def pop_queue(queue):
    filenames = os.listdir(queue)
    if filenames:
        filenames = sorted(filenames)
        filepath = '{}/{}'.format(queue, filenames[0])

        with open(filepath) as file:
            return (json.load(file), filepath)

    return None


if __name__ == '__main__':
    import time

    HOME = os.environ.get('HOME')
    queue = '{}/queue'.format(HOME)

    while True:
        ret = pop_queue(queue)

        if ret:
            (data, filepath) = ret
            options = data.get('options')
            steps = data.get('steps')

            print()
            try:
                execute(options, steps)
            except Exception as err:
                print(err)
            print()

            os.remove(filepath)
        else:
            print('Heartbeat ({} seconds)'.format(settings.HEARTBEAT))
            time.sleep(settings.HEARTBEAT)
