"""
Main module
"""
from pyspark.sql import SparkSession
from pyspark.sql.dataframe import DataFrame


APP_NAME = 'coding_example_run'


def filter_invalid_records(spark: SparkSession, input_df: DataFrame) -> DataFrame:
# add function definition here

def add_activity_lookup(spark: SparkSession, input_df: DataFrame, lookup_df: DataFrame) -> DataFrame:
  filtered_input = filter_invalid_records(spark, input_df)
  # complete the function definition here using filtered_input


def get_top_3_active_hours(spark: SparkSession, input_df: DataFrame) -> DataFrame:
  filtered_input = filter_invalid_records(spark, input_df)
  # complete the function definition here using filtered_input


def get_top_2_activity_codes(spark: SparkSession, input_df: DataFrame, lookup_df: DataFrame) -> DataFrame:
  input_with_activity_code = add_activity_lookup(spark, input_df, lookup_df)
  # complete the function definition here using input_with_activity_code


def get_activity_sessions_per_user(spark: SparkSession, input_df: DataFrame, lookup_df: DataFrame) -> DataFrame:
  input_with_activity_code = add_activity_lookup(spark, input_df, lookup_df)
  # complete the function definition here using input_with_activity_code


def main():
    print("Entering app..")
	# spark_session = SparkSession.builder.appName(APP_NAME).getOrCreate()
	# filter_invalid_records(spark_session, input_df, lookup_df)

if __name__ == '__main__':
    main()
