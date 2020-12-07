from pyspark.sql import SparkSession
from functools import lru_cache


@lru_cache(maxsize=None)
def get_spark():
    return (SparkSession.builder
            .master("local")
            .appName("ginkgo_mysql_to_redshift_etl")
            # .config("spark.driver.extraClassPath", "resources/mariadb-java-client-2.7.1.jar")
            # .config("spark.executor.extraClassPath", "resources/mariadb-java-client-2.7.1.jar")
            .getOrCreate())
