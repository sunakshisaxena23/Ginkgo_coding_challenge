from ginkgo_coding_challenge.spark_session_provider import get_spark
from ginkgo_coding_challenge import data_transformation as dt
from graphframes import *
from graphframes.lib import Pregel

import pyspark.sql.functions as f
from settings import db_user, db_password, url
import os

# os.environ["PYSPARK_PYTHON"] = "anaconda3/envs/untitled/bin/python"
# os.environ["PYSPARK_DRIVER_PYTHON"] = "anaconda3/envs/untitled/bin/python"

spark = get_spark()
spark.sparkContext.setCheckpointDir("/tmp/checkpoints")

samples_DF = spark.read \
    .format("jdbc") \
    .option("driver", "org.mariadb.jdbc.Driver") \
    .option("url", url) \
    .option("dbtable", "samples") \
    .option("user", db_user) \
    .option("password", db_password) \
    .load()

sample_measurements_DF = spark.read \
    .format("jdbc") \
    .option("driver", "org.mariadb.jdbc.Driver") \
    .option("url", url) \
    .option("dbtable", "sample_measurements") \
    .option("user", db_user) \
    .option("password", db_password) \
    .load()

# initialize the message column with own source id
vertices = samples_DF.select("id", "experiment_id", "parent_id")

edges = samples_DF \
    .select(f.col("parent_id").alias("src"), f.col("id").alias("dst"))

graph = GraphFrame(vertices, edges)

result = graph.pregel.withVertexColumn("path",
                                       f.lit(""),
                                       Pregel.msg()
                                       ) \
    .sendMsgToDst(f.concat_ws("|", Pregel.src("path"), Pregel.src("id"))) \
    .aggMsgs(f.collect_list(Pregel.msg())) \
    .setMaxIter(10) \
    .setCheckpointInterval(2) \
    .run()

samples_transformed_DF = dt.with_top_parent_id(result)

sample_measurements_pivoted_DF = dt.with_measurement_pivot(sample_measurements_DF)
sample_measurements_transformed_DF = dt.clean_df_names(sample_measurements_pivoted_DF)

select_query = dt.with_select_query(sample_measurements_transformed_DF)


final_measurements_transformed_DF = samples_transformed_DF \
    .join(sample_measurements_transformed_DF, ["sample_id"],
          "left") \
    .select(select_query) \

experiment_measurements_cols = spark.read \
    .format("jdbc") \
    .option("driver", "org.mariadb.jdbc.Driver") \
    .option("url", url) \
    .option("query", "select * from experiment_measurements limit 1") \
    .option("user", db_user) \
    .option("password", db_password) \
    .load()

create_table_query = dt.with_create_query(select_query, experiment_measurements_cols)

final_measurements_transformed_DF.write.mode("overwrite") \
    .option("createTableColumnTypes", create_table_query) \
    .format("jdbc") \
    .option("driver", "org.mariadb.jdbc.Driver") \
    .option("url", url) \
    .option("dbtable", "experiment_measurements") \
    .option("user", db_user) \
    .option("password", db_password) \
    .save()
