from functools import reduce
from typing import Optional, List, Dict
from pyspark.sql import DataFrame, Column, Window, functions as F

class DataModelCheck:
    @classmethod
    def uniqueness(cls, src: DataFrame, keys: List[str]) -> DataFrame:
        result_full = (
            src
            .groupBy(*keys)
            .agg(F.count("*").alias("_cardinality"))
        )
        return result_full.where(F.col("_cardinality") > 1)
    
    @classmethod
    def denormalization(cls, src: DataFrame, group: List[str], values: List[str]) -> DataFrame:
        group_cols = [F.col(c) for c in group]
        result_full = (
            src
            .select(*(group_cols + [F.struct(c).alias(c) for c in values]))
            .groupBy(*group_cols)
            .agg(*[F.collect_set(c).alias(c) for c in values])
            .select(*(group_cols + [F.array_sort(F.transform(c, lambda x: x.getField(c))).alias(c) for c in values]))
            .select(*(group_cols + [F.when(F.size(c) > 1, F.to_json(c)).alias(c) for c in values]))
        )
        return result_full.where(cls._any_null(values))
    
    @classmethod
    def relationship(cls, src: DataFrame, ref: DataFrame, keys: List[str], join: List[str], semantic: str) -> DataFrame:
        src_cols = keys + [x for x in join if x not in keys]
        src_prep = src.select(*src_cols)
        ref_prep = ref.select(*join).withColumn("_ref_exists", F.lit(1))
        result_full = (
            cls._join_null_safe(src_prep, ref_prep, join, how="left")
            .groupBy(*[src_prep[c] for c in src_cols])
            .agg(F.count("_ref_exists").alias("_cardinality"))
        )
        semantic_standard = semantic.lower().replace("-", "_").replace(" ", "_").replace("_once", "_one")
        if semantic_standard == "exactly_one":
            condition = F.col("_cardinality") != 1
        elif semantic_standard == "at_most_one":
            condition = F.col("_cardinality") > 1
        elif semantic_standard == "at_least_one":
            condition = F.col("_cardinality") < 1
        elif semantic_standard == "none":
            condition = F.col("_cardinality") >= 1
        else:
            raise ValueError(f"Semantic {semantic} not known.")
        return result_full.where(condition)
    
    @classmethod
    def consistency(cls, src: DataFrame, ref: DataFrame, join: List[str], values: List[str]) -> DataFrame:
        result_full = (
            cls._join_null_safe(src, ref, join, how="inner")
            .select(*(
                [src[c] for c in join] +
                [cls._not_equal_exact(src[c], ref[c]).alias(c) for c in values]
            ))
        )
        return result_full.where(cls._any_null(values))
    
    @classmethod
    def accuracy(
        cls, 
        src: DataFrame, 
        ref: DataFrame, 
        keys: List[str], 
        exact: Optional[List[str]] = None, 
        approx: Optional[Dict[str, str]] = None
    ) -> DataFrame:
        exact = exact or []
        approx = approx or {}
        src_prep = src.withColumn("_exists", F.lit(True))
        ref_prep = ref.withColumn("_exists", F.lit(True))
        result_full = (
            cls._join_null_safe(src_prep, ref_prep, keys, how="full_outer")
            .withColumn("_src_exists", F.coalesce(src_prep["_exists"], F.lit(False)))
            .withColumn("_ref_exists", F.coalesce(ref_prep["_exists"], F.lit(False)))
            .select(*(
                [F.coalesce(src_prep[c], ref_prep[c]).alias(c) for c in keys] +
                [cls._not_equal_exact(F.col("_src_exists"), F.col("_ref_exists")).alias("_exists")] +
                [cls._not_equal_exact(src_prep[c], ref_prep[c]).alias(c) for c in exact] +
                [cls._not_equal_approx(src_prep[c], ref_prep[c], F.expr(tol)).alias(c) for c, tol in approx.items()]
            ))
        )
        return result_full.where(cls._any_null(["_exists"] + exact + list(approx.keys())))
    
    @classmethod
    def accuracy_sorted(
        cls, 
        src: DataFrame, 
        ref: DataFrame, 
        sort: List[str], 
        group: Optional[List[str]] = None, 
        exact: Optional[List[str]] = None, 
        approx: Optional[Dict[str, str]] = None
    ) -> DataFrame:
        group = group or []
        window = Window.partitionBy(*group).orderBy(*sort) if group else Window.orderBy(*sort)
        result = cls.accuracy(
            src=src.withColumn("_rank", F.row_number().over(window)),
            ref=ref.withColumn("_rank", F.row_number().over(window)),
            keys = group + ["_rank"],
            exact=exact,
            approx=approx
        )
        return result
    
    @staticmethod
    def _join_null_safe(src: DataFrame, ref: DataFrame, join: List[str], how: str) -> DataFrame:
        condition = reduce(
            lambda x, y: x & y,
            [src[c].eqNullSafe(ref[c]) for c in join]
        )
        return src.join(ref, condition, how)
    
    @staticmethod
    def _find_latest(src: DataFrame, group: List[str], version: List[str]) -> DataFrame:
        latest = (
            src
            .withColumn("_version", F.struct(*version))
            .withColumn("_version_max", F.max("_version").over(Window.partitionBy(*group)))
            .where(F.col("_version") == F.col("_version_max"))
            .drop("_version", "_version_max")
        )
        return latest
    
    @staticmethod
    def _any_null(columns: List[str]) -> Column:
        return reduce(lambda x, y: x | y, [F.col(c).isNotNull() for c in columns])
    
    @staticmethod
    def _not_equal_exact(x: Column, y: Column) -> Column:
        return F.when(~x.eqNullSafe(y), F.to_json(F.array(x, y)))
    
    @staticmethod
    def _not_equal_approx(x: Column, y: Column, tol: Column) -> Column:
        return F.when(
            ~(
                (x.isNull() & y.isNull()) |
                F.coalesce(F.abs(x - y) <= tol, F.lit(False))
            ), 
            F.to_json(F.array(x, y))
        )
