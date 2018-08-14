# -*- coding: utf-8 -*-


from pyspark.sql import SparkSession


if __name__ == '__main__':
    spark_context = SparkSession.builder.appName('SpeedyGonzales').getOrCreate().sparkContext
    spark_context._conf.set("spark.executorEnv.JAVA_HOME", '/usr/lib/jvm/java-8-oracle')
    spark_context._conf.set("spark.driver.maxResultSize", '4g')

    rdd = spark_context.textFile('../README.md')
    print(rdd.collect())

    spark_context.stop()
