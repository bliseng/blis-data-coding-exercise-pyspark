"""
Test cases
"""
import pytest
from pyspark.sql import SparkSession
from pyspark.sql.dataframe import DataFrame
from pyspark.sql import functions as fn

from blis.main import (filter_invalid_records, 
                       add_activity_lookup, 
					   get_top_3_active_hours, 
					   get_top_2_activity_codes, 
					   get_activity_sessions_per_user)


@pytest.fixture(scope='module', name='input_df')
def get_test_input_df(spark_session: SparkSession, test_data_paths: dict) -> DataFrame:
  return spark_session.read.option('header', 'true').csv(test_data_paths['test_input_data'])


@pytest.fixture(scope='module', name='lookup_df')
def get_test_lookup_df(spark_session: SparkSession, test_data_paths: dict) -> DataFrame:
  return spark_session.read.option('header', 'true').csv(test_data_paths['lookup_data_path'])


def test_filter_invalid_records(input_df):
  op = filter_invalid_records(input_df)
  assert op.count() == 200


def test_add_activity_lookup(input_df, lookup_df):
  op = add_activity_lookup(input_df, lookup_df)
  assert op.filter(fn.col('activity_code') == 999).count() == 24
  assert op.count() == 200


def test_get_top_3_active_hours(input_df):
  op = get_top_3_active_hours(input_df)
  op_active_hours = [ row['hour'] for row in op.orderBy('hour').collect() ]
  assert op.count() == 3
  assert op_active_hours == [6, 8, 9]


def test_get_top_2_activity_codes(input_df, lookup_df):
  op = get_top_2_activity_codes(input_df, lookup_df)
  op_top_2_activity_codes = [ row['activity_code'] for row in op.orderBy('activity_code').collect() ]
  assert op.count() == 2
  assert op_top_2_activity_codes == [1, 3]


def test_get_activity_sessions_per_user(input_df, lookup_df):
  op = get_activity_sessions_per_user(input_df, lookup_df)
  assert op.count() == 67
