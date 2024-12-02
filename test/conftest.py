import os
import shutil

import pytest
from pyspark.sql import SparkSession

APP_NAME = 'func-testing'
RESOURCES_DIR = os.path.join('test', 'resources')
TEST_DATA_PATH = os.path.join(RESOURCES_DIR, 'input')
LOOKUP_DATA_PATH = os.path.join(RESOURCES_DIR, 'lookup')


@pytest.fixture(name='test_folder', scope='session')
def create_test_folder(tmp_path_factory):
    """
    This fixture creates a temporary test directory and deletes it after the test.
    """
    d = tmp_path_factory.mktemp('op_files')
    yield str(d)
    shutil.rmtree(d, True)


@pytest.fixture(scope='session', name='spark_session')
def pyspark_app() -> SparkSession:
    spark = SparkSession.builder.appName(APP_NAME).getOrCreate()
    yield spark


@pytest.fixture(scope='session', name='test_data_paths')
def create_test_data_paths(test_folder):
    yield {
		'test_input_data': TEST_DATA_PATH,
		'lookup_data_path': LOOKUP_DATA_PATH,
		'op_path': test_folder
	}
