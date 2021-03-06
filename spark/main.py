# -*- coding: utf-8 -*-


import os
from pyspark.sql import SparkSession
import requests as r
import sys

from sequencers import BlockSequenceBuilder
import settings


def execute(job_id, options, steps):
    print("Job '{}' started".format(job_id))

    with open('job_id', 'w') as f:
        f.write(job_id)

    try:
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

    except Exception as err:
        print(err)

    finally:
        spark_context.stop()
        os.remove('job_id')

    print("Job '{}' finished".format(job_id))


def pop_queue():
    resp = r.get('http://localhost:8000/jobs/pop')
    if resp.status_code == 200:
        print('POP!')
        return resp.json()
    return None


def exit():
    pid_filepath = '{}/spark.pid'.format(settings.SUBROOT_PATH)
    os.remove(pid_filepath)
    print('Bye bye beautiful!')
    sys.exit()


if __name__ == '__main__':
    import time

    num_sleeps = 0

    while True:
        ret = pop_queue()

        if ret:
            num_sleeps = 0

            job_id = ret.get('job_id')
            options = ret.get('options')
            steps = ret.get('steps')

            try:
                execute(job_id, options, steps)
            except Exception as err:
                print(err)

        else:
            num_sleeps = num_sleeps + 1
            if num_sleeps == settings.MAX_SLEEPS:
                exit()

            print('Sleeping for {} seconds'.format(settings.SLEEP_INTERVAL))
            time.sleep(settings.SLEEP_INTERVAL)
