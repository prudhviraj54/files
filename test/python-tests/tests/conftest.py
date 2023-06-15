import sys
import os
import pytest
from pyspark.sql import SparkSession
import logging

@pytest.fixture(scope='session')
def spark():
    return (SparkSession.builder 
            .master("local") 
            .appName("nifi_migration") 
            .getOrCreate())

@pytest.fixture
def logger():
    return logging.getLogger('nifi.migration.test')

@pytest.fixture
def test_dataframe(spark):
    data = (
      [{"key":"keyI","value":"valueI"},
      {"key":"keyII","value":"valueII"},
      {"key":"keyIII","value":"valueIII"}])

    return spark.read.json(spark.sparkContext.parallelize([data]))

def pytest_sessionstart():
    sys.path.insert(0, os.path.abspath('src'))
    os.chdir('src')

def pytest_runtest_setup():
    print('pytest_runtest_setup')
