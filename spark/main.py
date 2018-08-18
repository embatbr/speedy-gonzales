# -*- coding: utf-8 -*-


import json
import os
from pyspark.sql import SparkSession
import sys

from sequencers import BlockSequenceBuilder
import settings


def execute(options, steps, filename):
    with open('job_id', 'w') as f:
        f.write(filename)

    spark_context = SparkSession.builder.appName('speedy-gonzales').getOrCreate().sparkContext
    spark_context._conf.set("spark.executorEnv.JAVA_HOME", settings.JAVA_HOME)
    spark_context._conf.set("spark.driver.maxResultSize", settings.MAX_RESULT_SIZE)
    if options and ('s3' in options):
        spark_context._jsc.hadoopConfiguration().set("fs.s3n.impl", "org.apache.hadoop.fs.s3native.NativeS3FileSystem")
        spark_context._jsc.hadoopConfiguration().set("fs.s3n.awsAccessKeyId", options['s3']['aws_access_key_id'])
        spark_context._jsc.hadoopConfiguration().set("fs.s3n.awsSecretAccessKey", options['s3']['aws_secret_access_key'])

    block_sequence_builder = BlockSequenceBuilder(spark_context)
    block_sequence_executor = block_sequence_builder.build(steps)

    block_sequence_executor.execute()
    print(json.dumps(block_sequence_executor.memory['dataset'], indent=4))

    spark_context.stop()

    os.remove('job_id')


def pop_queue(queue):
    filenames = os.listdir(queue)
    if filenames:
        filenames = sorted(filenames)
        filename = filenames[0]
        filepath = '{}/{}'.format(queue, filename)

        with open(filepath) as file:
            return (json.load(file), filepath, filename)

    return None


if __name__ == '__main__':
    import time

    queue = '/tmp/queue'

    while True:
        ret = pop_queue(queue)

        if ret:
            (data, filepath, filename) = ret
            options = data.get('options')
            steps = data.get('steps')

            os.remove(filepath)

            print()
            try:
                execute(options, steps, filename)
            except Exception as err:
                print(err)
            print()
        else:
            print('Heartbeat ({} seconds)'.format(settings.HEARTBEAT))
            time.sleep(settings.HEARTBEAT)
