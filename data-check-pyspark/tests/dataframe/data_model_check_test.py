from pyspark.sql import SparkSession

from data_check.dataframe.data_model_check import DataCheck
from ..spark_utils import *


def test_uniqueness(spark: SparkSession):
    order_details = spark.sql("""
        values 
        (1, 'a', 'x'), 
        (1, 'b', 'x'), 
        (2, 'b', 'x'), 
        (null, 'a', 'x'), 
        (1, null, 'x'), 
        (3, 'c', 'x'), 
        (3, 'c', 'x'), 
        (4, null, 'x'), 
        (4, null, 'x'), 
        (4, null, 'x'), 
        (null, 'e', 'x'), 
        (null, 'e', 'x'), 
        (null, null, 'x'), 
        (null, null, 'x') 
        as (order_id, item_id, comment)
    """)

    result = DataCheck.uniqueness(
        src=order_details, 
        keys=["order_id", "item_id"]
    )

    expected = spark.sql("""
        values
        (3, 'c', 2),
        (4, null, 3),
        (null, 'e', 2),
        (null, null, 2) as
        (order_id, item_id, _cardinality)
    """)

    assert_df_equals(result, expected)


def test_denormalization(spark: SparkSession):
    promotion_details = spark.sql("""
        values
        ('a', 1, 'x', 101, 'c01'),
        ('a', 2, 'x', 102, 'c01'), ('a', 2, 'x', 102, 'c02'),
        ('b', 1, 'x', 103, 'c01'), ('b', 1, 'x', 103, 'c02'),
        ('b', 2, 'x', 104, 'c01'), ('b', 2, 'x', 105, 'c02'), ('b', 2, 'x', 105, 'c03'),
        ('b', 3, 'x', 106, 'c01'), ('b', 3, 'y', 106, 'c02'), ('b', 3, 'z', 106, 'c03'),
        ('c', 1, 'x', 107, 'c01'), ('c', 1, 'x', 108, 'c02'), ('c', 1, 'x', null, 'c03'),
        ('c', 2, 'x', 109, 'c01'), ('c', 2, null, 109, 'c02'),
        ('d', 1, 'x', 110, 'c01'), ('d', 1, null, 111, 'c02'), ('d', 1, 'x', 111, 'c03'), ('d', 1, null, 110, 'c04'),
        ('e', null, 'x', 112, 'c01'), ('e', null, 'x', 113, 'c02'),
        (null, 1, 'x', 114, 'c01'), (null, 1, null, 114, 'c02'),
        (null, null, 'x', 115, 'c01'), (null, null, null, null, 'c02')
        as (article_id, supplier_id, category_name, supplier_phone, comment)
    """)

    result = DataCheck.denormalization(
        src=promotion_details,
        group=["article_id", "supplier_id"],
        values=["category_name", "supplier_phone"],
    )

    expected = spark.sql("""
        values
        ('b', 2, null, '[104,105]'),
        ('b', 3, '["x","y","z"]', null),
        ('c', 1, null, '[107,108,null]'),
        ('c', 2, '["x",null]', null),
        ('d', 1, '["x",null]', '[110,111]'),
        ('e', null, null, '[112,113]'),
        (null, 1, '["x",null]', null),
        (null, null, '["x",null]', '[115,null]')
        as (article_id, supplier_id, category_name, supplier_phone)
    """)

    assert_df_equals(result, expected)
    

def test_relationship(spark: SparkSession):
    order_details = spark.sql("""
        values
        ('o1', 'a1', 1, 8),
        ('o2', 'a1', 2, 8),
        ('o2', 'a2', 1, 8),
        ('o3', null, 1, 8),
        ('o4', 'a3', null, 8)
        as (order_id, article_id, promotion_id, order_count)
    """)

    promotion_details = spark.sql("""
        values
        ('a1', 2, 7.0),
        ('a2', 1, 7.0), ('a2', 1, 9.0),
        ('a3', null, 7.0), ('a3', null, 9.0)
        as (article_id, promotion_id, unit_price)
    """)

    result = DataCheck.relationship(
        src=order_details,
        ref=promotion_details,
        keys=["order_id", "article_id"],
        join=["article_id", "promotion_id"],
        semantic="exactly_one",
    )

    expected = spark.sql("""
        values
        ('o1', 'a1', 1, 0),
        ('o2', 'a2', 1, 2),
        ('o3', null, 1, 0),
        ('o4', 'a3', null, 2)
        as (order_id, article_id, promotion_id, _cardinality)
    """)

    assert_df_equals(result, expected)

    result = DataCheck.relationship(
        src=order_details,
        ref=promotion_details,
        keys=["order_id", "article_id"],
        join=["article_id", "promotion_id"],
        semantic="at_most_one",
    )

    expected = spark.sql("""
        values
        ('o2', 'a2', 1, 2),
        ('o4', 'a3', null, 2)
        as (order_id, article_id, promotion_id, _cardinality)
    """)

    assert_df_equals(result, expected)

    result = DataCheck.relationship(
        src=order_details,
        ref=promotion_details,
        keys=["order_id", "article_id"],
        join=["article_id", "promotion_id"],
        semantic="at_least_one",
    )

    expected = spark.sql("""
        values
        ('o1', 'a1', 1, 0),
        ('o3', null, 1, 0)
        as (order_id, article_id, promotion_id, _cardinality)
    """)

    assert_df_equals(result, expected)

    result = DataCheck.relationship(
        src=order_details,
        ref=promotion_details,
        keys=["order_id", "article_id"],
        join=["article_id", "promotion_id"],
        semantic="none",
    )

    expected = spark.sql("""
        values
        ('o2', 'a1', 2, 1),
        ('o2', 'a2', 1, 2),
        ('o4', 'a3', null, 2)
        as (order_id, article_id, promotion_id, _cardinality)
    """)

    assert_df_equals(result, expected)


def test_consistency(spark: SparkSession):
    order_details = spark.sql("""
        values
        ('a', 0, 'x', 101, 'c01'),
        ('a', 1, 'x', 101, 'c01'),
        ('a', 2, 'x', 101, 'c01'),
        ('b', 1, 'x', 101, 'c01'),
        ('b', 2, 'x', 101, 'c01'),
        ('c', 1, 'x', 101, 'c01'), ('c', 1, 'x', 102, 'c01'), ('c', 1, 'x', 103, 'c01'),
        ('d', 1, 'x', 101, 'c01'),
        ('e', null, 'x', 101, 'c01'),
        (null, 1, 'x', 101, 'c01'),
        (null, null, 'x', 101, 'c01')
        as (article_id, promotion_id, category_name, supplier_phone, comment)
    """)

    promotion_details = spark.sql("""
        values
        ('a', 1, 'x', 101, 'c01'),
        ('a', 2, 'x', 102, 'c01'),
        ('a', 3, 'x', 101, 'c01'),
        ('b', 1, 'y', 101, 'c01'),
        ('b', 2, 'y', 102, 'c01'),
        ('c', 1, 'x', 101, 'c01'),
        ('d', 1, 'y', 101, 'c01'), ('d', 1, 'z', 101, 'c01'),
        ('e', null, 'x', 102, 'c01'),
        (null, 1, 'x', 102, 'c01'),
        (null, null, 'x', 102, 'c01')
        as (article_id, promotion_id, category_name, supplier_phone, comment)
    """)

    result = DataCheck.consistency(
        src=order_details,
        ref=promotion_details,
        join=["article_id", "promotion_id"],
        values=["category_name", "supplier_phone"],
    )

    expected = spark.sql("""
        values
        ('a', 2, null, '[101,102]'),
        ('b', 1, '["x","y"]', null),
        ('b', 2, '["x","y"]', '[101,102]'),
        ('c', 1, null, '[102,101]'),
        ('c', 1, null, '[103,101]'),
        ('d', 1, '["x","y"]', null),
        ('d', 1, '["x","z"]', null),
        ('e', null, null, '[101,102]'),
        (null, 1, null, '[101,102]'),
        (null, null, null, '[101,102]')
        as (article_id, promotion_id, category_name, supplier_phone)
    """)

    assert_df_equals(result, expected)


def test_accuracy(spark: SparkSession):
    real_result = spark.sql("""
        values
        ('2023-03-15', 1, '2023-03', 4, 12.7300, 2.5100, 'c02'),
        ('2023-03-15', 3, '2023-03', 4, 12.7300, 2.5100, 'c01'),
        ('2023-03-25', 1, '2023-03', 4, 12.7300, 2.5100, 'c01'),
        ('2023-04-03', 1, '2023-04', 4, 12.7300, 2.5100, 'c01'),
        ('2023-04-07', 1, '2023-04', 4, 12.7301, 2.5100, 'c01'),
        ('2023-04-11', 1, '2023-04', 4, 12.7300, 2.5110, 'c01'),
        ('2023-04-11', 2, '2023-04', 4, 12.7300, 2.5130, 'c01'),
        ('2023-04-11', 3, '2023-04', 4, 12.7300, 2.5300, 'c01'),
        ('2023-04-11', 4, '2023-04', 4, 12.7300, 2.5301, 'c01'),
        ('2023-04-11', 5, '2023-04', 4, 12.7300, 2.5099, 'c01'),
        ('2023-04-14', 1, '2023-04', 5, 12.7301, 2.5200, 'c01'),
        ('2023-04-17', null, '2023-04', 6, 12.7300, 2.5200, 'c01'),
        (null, 1, '2023-04', 7, 12.7300, 2.5200, 'c01'),
        (null, null, '2023-04', 8, 12.7300, 2.5200, 'c01')
        as (order_date, category_id, order_month, order_counts, sales_total, sales_avg, comment)
    """)

    expected_result = spark.sql("""
        values
        ('2023-03-15', 1, '2023-03', 4, 12.7300, 2.5100, 'c01'),
        ('2023-03-15', 2, '2023-03', 4, 12.7300, 2.5100, 'c01'),
        ('2023-03-25', 1, '2023-04', 4, 12.7300, 2.5100, 'c01'),
        ('2023-04-03', 1, '2023-04', 5, 12.7300, 2.5100, 'c01'),
        ('2023-04-07', 1, '2023-04', 4, 12.7300, 2.5100, 'c01'),
        ('2023-04-11', 1, '2023-04', 4, 12.7300, 2.5100, 'c01'),
        ('2023-04-11', 2, '2023-04', 4, 12.7300, 2.5170, 'c01'),
        ('2023-04-11', 3, '2023-04', 4, 12.7300, 2.5200, 'c01'),
        ('2023-04-11', 4, '2023-04', 4, 12.7300, 2.5200, 'c01'),
        ('2023-04-11', 5, '2023-04', 4, 12.7300, 2.5200, 'c01'),
        ('2023-04-14', 1, '2023-04', 4, 12.7300, 2.5200, 'c01'),
        ('2023-04-17', null, '2023-04', 4, 12.7300, 2.5200, 'c01'),
        (null, 1, '2023-04', 4, 12.7300, 2.5200, 'c01'),
        (null, null, '2023-04', 4, 12.7300, 2.5200, 'c01')
        as (order_date, category_id, order_month, order_counts, sales_total, sales_avg, comment)
    """)

    result = DataCheck.accuracy(
        src=real_result,
        ref=expected_result,
        keys=["order_date", "category_id"],
        exact=["order_month", "order_counts"],
        approx={"sales_total": "1e-12", "sales_avg": "0.01"},
    )

    expected = spark.sql("""
        values
        ('2023-03-15', 2, '[false,true]', '[null,"2023-03"]', '[null,4]', '[null,12.7300]', '[null,2.5100]'),
        ('2023-03-15', 3, '[true,false]', '["2023-03",null]', '[4,null]', '[12.7300,null]', '[2.5100,null]'),
        ('2023-03-25', 1, null, '["2023-03","2023-04"]', null, null, null),
        ('2023-04-03', 1, null, null, '[4,5]', null, null),
        ('2023-04-07', 1, null, null, null, '[12.7301,12.7300]', null),
        ('2023-04-11', 4, null, null, null, null, '[2.5301,2.5200]'),
        ('2023-04-11', 5, null, null, null, null, '[2.5099,2.5200]'),
        ('2023-04-14', 1, null, null, '[5,4]', '[12.7301,12.7300]', null),
        ('2023-04-17', null, null, null, '[6,4]', null, null),
        (null, 1, null, null, '[7,4]', null, null),
        (null, null, null, null, '[8,4]', null, null)
        as (order_date, category_id, _exists, order_month, order_counts, sales_total, sales_avg)
    """)

    assert_df_equals(result, expected)

    result = DataCheck.accuracy(
        src=real_result,
        ref=expected_result,
        keys=["order_date", "category_id"]
    )

    expected = spark.sql("""
        values
        ('2023-03-15', 2, '[false,true]'),
        ('2023-03-15', 3, '[true,false]')
        as (order_date, category_id, _exists)
    """)

    assert_df_equals(result, expected)


def test_accuracy_sorted(spark: SparkSession):
    real_result = spark.sql("""
        values
        ('2023-03-15', 1, '2023-03', 4, 12.7300, 2.5100, 'c02'),
        ('2023-03-15', 3, '2023-03', 4, 12.7300, 2.5100, 'c01'),
        ('2023-03-17', 1, '2023-03', 5, 12.7300, 2.5170, 'c01'),
        ('2023-03-17', 1, '2023-03', 6, 12.7300, 2.5210, 'c01'),
        ('2023-03-19', null, '2023-03', 7, 12.7301, 2.5100, 'c01'),
        (null, 1, '2023-03', 8, 12.7301, 2.5100, 'c01'),
        (null, null, '2023-03', 9, 12.7301, 2.5100, 'c01'),
        ('2023-03-21', 1, '2023-03', 14, 12.7300, 2.5100, 'c01'),
        ('2023-03-21', 1, '2023-03', 15, 12.7300, 2.5100, 'c01'),
        ('2023-03-21', 1, '2023-03', 16, 12.7300, 2.5100, 'c01')
        as (order_date, category_id, order_month, order_counts, sales_total, sales_avg, comment)
    """)

    expected_result = spark.sql("""
        values
        ('2023-03-15', 1, '2023-03', 4, 12.7300, 2.5100, 'c01'),
        ('2023-03-15', 2, '2023-03', 4, 12.7300, 2.5100, 'c01'),
        ('2023-03-17', 1, '2023-03', 5, 12.7300, 2.5100, 'c01'),
        ('2023-03-17', 1, '2023-03', 6, 12.7300, 2.5100, 'c01'),
        ('2023-03-19', null, '2023-03', 7, 12.7300, 2.5100, 'c01'),
        (null, 1, '2023-03', 8, 12.7300, 2.5100, 'c01'),
        (null, null, '2023-03', 9, 12.7300, 2.5100, 'c01'),
        ('2023-03-21', 1, '2023-03', 14, 12.7300, 2.5100, 'c01'),
        ('2023-03-21', 1, '2023-03', 15, 12.7300, 2.5100, 'c01'),
        ('2023-03-21', 1, '2023-03', 17, 12.7300, 2.5100, 'c01'),
        ('2023-03-21', 1, '2023-03', 18, 12.7300, 2.5100, 'c01')
        as (order_date, category_id, order_month, order_counts, sales_total, sales_avg, comment)
    """)

    result = DataCheck.accuracy_sorted(
        src=real_result,
        ref=expected_result,
        group=["order_date", "category_id"],
        sort=["order_counts", "sales_total"],
        exact=["order_month", "order_counts"],
        approx={"sales_total": "1e-12", "sales_avg": "0.01"},
    )

    expected = spark.sql("""
        values
        ('2023-03-15', 2, 1, '[false,true]', '[null,"2023-03"]', '[null,4]', '[null,12.7300]', '[null,2.5100]'),
        ('2023-03-15', 3, 1, '[true,false]', '["2023-03",null]', '[4,null]', '[12.7300,null]', '[2.5100,null]'),
        ('2023-03-17', 1, 2, null, null, null, null, '[2.5210,2.5100]'),
        ('2023-03-19', null, 1, null, null, null, '[12.7301,12.7300]', null),
        (null, 1, 1, null, null, null, '[12.7301,12.7300]', null),
        (null, null, 1, null, null, null, '[12.7301,12.7300]', null),
        ('2023-03-21', 1, 3, null, null, '[16,17]', null, null),
        ('2023-03-21', 1, 4, '[false,true]', '[null,"2023-03"]', '[null,18]', '[null,12.7300]', '[null,2.5100]')
        as (order_date, category_id, _rank, _exists, order_month, order_counts, sales_total, sales_avg)
    """)

    assert_df_equals(result, expected)

    result = DataCheck.accuracy_sorted(
        src=real_result,
        ref=expected_result,
        group=["order_date", "category_id"],
        sort=["order_counts", "sales_total"],
    )

    expected = spark.sql("""
        values
        ('2023-03-15', 2, 1, '[false,true]'),
        ('2023-03-15', 3, 1, '[true,false]'),
        ('2023-03-21', 1, 4, '[false,true]')
        as (order_date, category_id, _rank, _exists)
    """)

    assert_df_equals(result, expected)

    result = DataCheck.accuracy_sorted(
        src=real_result,
        ref=expected_result,
        sort=["order_counts", "sales_total"],
        exact=["order_month", "order_counts"],
        approx={"sales_total": "1e-12", "sales_avg": "0.01"},
    )

    expected = spark.sql("""
        values
        (4, null, null, null, null, '[2.5210,2.5100]'),
        (5, null, null, null, '[12.7301,12.7300]', null),
        (6, null, null, null, '[12.7301,12.7300]', null),
        (7, null, null, null, '[12.7301,12.7300]', null),
        (10, null, null, '[16,17]', null, null),
        (11, '[false,true]', '[null,"2023-03"]', '[null,18]', '[null,12.7300]', '[null,2.5100]')
        as (_rank, _exists, order_month, order_counts, sales_total, sales_avg)
    """)

    assert_df_equals(result, expected)
