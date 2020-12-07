import pytest

from ginkgo_coding_challenge.spark_session_provider import get_spark
from ginkgo_coding_challenge.data_transformation import with_top_parent_id, with_measurement_pivot, clean_df_names

import os

os.environ["PYSPARK_PYTHON"] = "/Users/sunakshisaxena/anaconda3/envs/untitled/bin/python"
os.environ["PYSPARK_DRIVER_PYTHON"] = "/Users/sunakshisaxena/anaconda3/envs/untitled/bin/python"


class TestDataTransformation(object):

    def test_with_top_parent_id(self):
        source_data = [
            (["1|4|6"]), (["13|4|6"]), (["13"])
        ]
        source_df = get_spark().createDataFrame(
            source_data,
            ["path"]
        )

        actual_df = with_top_parent_id(source_df)

        expected_data = [
            ("1"), ("13"), ("13")
        ]
        expected_df = get_spark().createDataFrame(
            expected_data,
            ["top_parent_id"]
        )

        assert (expected_df.collect() == actual_df.collect())

    def test_with_measurement_pivot(self):
        source_data = [
            (2, 'vol', 500.0), (3, 'vol', 400.0),
            (6, 'vol', 51.0), (9, 'vol', 50.0),
            (10, 'vol', 10.5), (12, 'vol', 40.3),
            (17, 'vol', 10.2), (8, 'vol', 40.8),
            (19, 'vol', 10.0), (20, 'vol', 40.7),
            (2, 'ph', 5.0), (3, 'ph', 7.0),
            (6, 'ph', 5.1), (9, 'ph', 7.2),
            (10, 'ph', 5.2), (12, 'ph', 7.4),
            (17, 'ph', 5.0), (8, 'ph', 7.4),
            (19, 'ph', 5.25), (20, 'ph', 7.34)
        ]
        source_df = get_spark().createDataFrame(
            source_data,
            ["sample_id", "measurement_type", "value"]
        )

        actual_df = with_measurement_pivot(source_df).select("sample_id", "vol", "ph").orderBy("sample_id")

        expected_data = [
            (2, 500.0, 5.0), (3, 400.0, 7.0),
            (6, 51.0, 5.1), (8, 40.8, 7.4),
            (9, 50.0, 7.2), (10, 10.5, 5.2),
            (12, 40.3, 7.4), (17, 10.2, 5.0),
            (19, 10.0, 5.25), (20, 40.7, 7.34)
        ]
        expected_df = get_spark().createDataFrame(
            expected_data,
            ["sample_id", "vol", "ph"]
        )

        assert (expected_df.collect() == actual_df.collect())

    def test_clean_df_names(self):
        source_data = [
            (2, 500.0, 5.0), (3, 400.0, 7.0),
            (6, 51.0, 5.1), (8, 40.8, 7.4),
            (9, 50.0, 7.2), (10, 10.5, 5.2),
            (12, 40.3, 7.4), (17, 10.2, 5.0),
            (19, 10.0, 5.25), (20, 40.7, 7.34)
        ]
        source_df = get_spark().createDataFrame(
            source_data,
            ["sample_id", "vol", "ph"]
        )

        actual_df = clean_df_names(source_df)\
            .select("sample_id", "measurement_vol", "measurement_ph")\
            .orderBy("sample_id")

        expected_data = [
            (2, 500.0, 5.0), (3, 400.0, 7.0),
            (6, 51.0, 5.1), (8, 40.8, 7.4),
            (9, 50.0, 7.2), (10, 10.5, 5.2),
            (12, 40.3, 7.4), (17, 10.2, 5.0),
            (19, 10.0, 5.25), (20, 40.7, 7.34)
        ]
        expected_df = get_spark().createDataFrame(
            expected_data,
            ["sample_id", "measurement_vol", "measurement_ph"]
        )

        assert (expected_df.collect() == actual_df.collect())
