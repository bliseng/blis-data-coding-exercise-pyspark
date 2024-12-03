"""
Test cases
"""
from pyspark.sql import SparkSession
from pyspark.sql.dataframe import DataFrame

from blis.main import (filter_invalid_records, 
                       add_activity_lookup, 
					   get_top_3_active_hours, 
					   get_top_2_activity_codes, 
					   get_activity_sessions_per_user)


@pytest.fixture(scope='module', name='input_df')
def get_test_input_df(spark_session: SparkSession, test_data_paths: dict) -> DataFrame:
  retrun spark_session.read.option('header', 'true').csv(test_data_paths['test_input_data'])


@pytest.fixture(scope='module', name='lookup_df')
def get_test_lookup_df(spark_session: SparkSession, test_data_paths: dict) -> DataFrame:
  retrun spark_session.read.option('header', 'true').csv(test_data_paths['lookup_data_path'])


def test_filter_invalid_records(spark_session, input_df):
  op = filter_invalid_records(spark_session, input_df)
  assert op.count() == 200


def test_add_activity_lookup(spark_session, input_df, lookup_df):
  op = add_activity_lookup(spark_session, input_df, lookup_df)
  assert op.count() == 200


def test_get_top_3_active_hours(spark_session, input_df):
  op = get_top_3_active_hours(spark_session, input_df)
  assert op.count() == 3
  assert op_active_hours == [9, 12, 23]


def test_get_top_2_activity_codes(spark_session, input_df, lookup_df):
  op = get_top_2_activity_codes(spark_session, input_df, lookup_df)
  assert op.count() == 2
  assert op_active_hours == [1, 2]


def test_get_activity_sessions_per_user(spark_session, input_df, lookup_df):
  op = get_activity_sessions_per_user(spark_session, input_df, lookup_df)
  assert op.count() == 36
