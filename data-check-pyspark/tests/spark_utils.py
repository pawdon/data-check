import pytest
from pyspark.sql import SparkSession, DataFrame


@pytest.fixture(scope="session")
def spark() -> SparkSession:
    session = (
      SparkSession
        .builder
        .master("local[*]")
        .appName("spark test")
        .config("spark.driver.bindAddress", "127.0.0.1")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.log.level", "WARN")
        .getOrCreate()
    )
    yield session
    session.stop()


def assert_df_equals(df1: DataFrame, df2: DataFrame):
    assert df1.columns == df2.columns, "Columns should be equal"
    assert df1.orderBy(*df1.columns).collect() == df2.orderBy(*df2.columns).collect(), "Rows should be equal"
