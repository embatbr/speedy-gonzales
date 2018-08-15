# -*- coding: utf-8 -*-


import json
from pyspark.sql import SparkSession
import sys

from sequencers import SequenceBuilder
import settings
# TODO import function to create script from exec params


def execute(steps):
    spark_context = SparkSession.builder.appName('speedy-gonzales').getOrCreate().sparkContext
    spark_context._conf.set("spark.executorEnv.JAVA_HOME", settings.JAVA_HOME)
    spark_context._conf.set("spark.driver.maxResultSize", settings.MAX_RESULT_SIZE)

    sequence_builder = SequenceBuilder(spark_context)
    sequence_executor = sequence_builder.build(steps)

    sequence_executor.execute()

    spark_context.stop()


if __name__ == '__main__':
    params = sys.argv[1]
    params = json.loads(params)

    steps = params.get('steps')

    execute(steps)
