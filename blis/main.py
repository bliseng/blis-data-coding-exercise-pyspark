"""
Main module
"""
from pyspark.sql import SparkSession
from pyspark.sql.dataframe import DataFrame


APP_NAME = 'coding_example_run'


def filter_invalid_records(input_df: DataFrame) -> DataFrame:
# add function implementation here


def add_activity_lookup(input_df: DataFrame, lookup_df: DataFrame) -> DataFrame:
  filtered_input = filter_invalid_records(input_df)
  # complete the function implementation here using filtered_input


def get_top_3_active_hours(input_df: DataFrame) -> DataFrame:
  filtered_input = filter_invalid_records(input_df)
  # complete the function implementation here using filtered_input


def get_top_2_activity_codes(input_df: DataFrame, lookup_df: DataFrame) -> DataFrame:
  input_with_activity_code = add_activity_lookup(input_df, lookup_df)
  # complete the function implementation here using input_with_activity_code


def get_activity_sessions_per_user(input_df: DataFrame, lookup_df: DataFrame) -> DataFrame:
  input_with_activity_code = add_activity_lookup(input_df, lookup_df)
  # complete the function implementation here using input_with_activity_code


# this is just a dummy main function, which doesn't need to be implemented.
def main():
    print("Entering app..")
	# spark_session = SparkSession.builder.appName(APP_NAME).getOrCreate()
	# filter_invalid_records(input_df)

if __name__ == '__main__':
    main()
