package com.pawdon.datacheck

import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.{Column, DataFrame, functions => F}

object DataModelCheck {
  def uniqueness(src: DataFrame, keys: Seq[String]): DataFrame = {
    val resultFull = src
      .groupBy(keys map F.col: _*)
      .agg(F.count("*").alias("_cardinality"))
    resultFull.where(F.col("_cardinality") > 1)
  }

  def denormalization(src: DataFrame, group: Seq[String], values: Seq[String]): DataFrame = {
    val groupCols = group map F.col
    val aggCols = values.map(c => F.collect_set(c).alias(c))
    val resultFull = src
      .select(groupCols ++ values.map(c => F.struct(c).alias(c)): _*)
      .groupBy(groupCols: _*)
      .agg(aggCols.head, aggCols.tail: _*)
      .select(groupCols ++ values.map(c => F.array_sort(F.transform(F.col(c), x => x.getField(c))).alias(c)): _*)
      .select(groupCols ++ values.map(c => F.when(F.size(F.col(c)) > 1, F.to_json(F.col(c))).alias(c)): _*)
    resultFull.where(anyNull(values))
  }

  def relationship(src: DataFrame, ref: DataFrame, keys: Seq[String], join: Seq[String], semantic: String): DataFrame = {
    val srcCols = (keys ++ join).distinct
    val srcPrep = src.select(srcCols map F.col: _*)
    val refPrep = ref.select(join map F.col: _*).withColumn("_ref_exists", F.lit(1))
    val group = srcCols.map(c => srcPrep(c))
    val resultFull = joinNullSafe(srcPrep, refPrep, join, "left")
      .groupBy(group: _*)
      .agg(F.count("_ref_exists").alias("_cardinality"))
    val semanticStandard = semantic
      .toLowerCase
      .replace("-", "_")
      .replace(" ", "_")
      .replace("_once", "_one")
    val condition = semanticStandard match {
      case "exactly_one" => F.col("_cardinality") =!= 1
      case "at_most_one" => F.col("_cardinality") > 1
      case "at_least_one" => F.col("_cardinality") < 1
      case "none" => F.col("_cardinality") >= 1
    }
    resultFull.where(condition)
  }

  def consistency(src: DataFrame, ref: DataFrame, join: Seq[String], values: Seq[String]): DataFrame = {
    val resultFull = joinNullSafe(src, ref, join, "inner")
      .select(join.map(c => src(c)) ++ values.map(c => notEqualExact(src(c), ref(c)).alias(c)): _*)
    resultFull.where(anyNull(values))
  }

  def accuracy(
                src: DataFrame,
                ref: DataFrame,
                keys: Seq[String],
                exact: Seq[String] = Seq.empty,
                approx: Map[String, String] = Map.empty
              ): DataFrame = {
    val srcPrep = src.withColumn("_exists", F.lit(true))
    val refPrep = ref.withColumn("_exists", F.lit(true))
    val resultFull = joinNullSafe(srcPrep, refPrep, keys, "full_outer")
      .withColumn("_src_exists", F.coalesce(srcPrep("_exists"), F.lit(false)))
      .withColumn("_ref_exists", F.coalesce(refPrep("_exists"), F.lit(false)))
      .select(
        keys.map(c => F.coalesce(srcPrep(c), refPrep(c)).alias(c)) ++
          Seq(notEqualExact(F.col("_src_exists"), F.col("_ref_exists")).alias("_exists")) ++
          exact.map(c => notEqualExact(srcPrep(c), refPrep(c)).alias(c)) ++
          approx.map({ case (c, tol) => notEqualApprox(srcPrep(c), refPrep(c), F.expr(tol)).alias(c) }): _*
      )
    resultFull.where(anyNull(Seq("_exists") ++ exact ++ approx.keys))
  }

  def accuracySorted(
                      src: DataFrame,
                      ref: DataFrame,
                      sort: Seq[String],
                      group: Seq[String] = Seq.empty,
                      exact: Seq[String] = Seq.empty,
                      approx: Map[String, String] = Map.empty
                    ): DataFrame = {
    val window = if (group.nonEmpty) {
      Window.partitionBy(group map F.col: _*).orderBy(sort map F.col: _*)
    } else {
      Window.orderBy(sort map F.col: _*)
    }
    accuracy(
      src = src.withColumn("_rank", F.row_number().over(window)),
      ref = ref.withColumn("_rank", F.row_number().over(window)),
      keys = group :+ "_rank",
      exact = exact,
      approx = approx
    )
  }

  private def joinNullSafe(src: DataFrame, ref: DataFrame, join: Seq[String], how: String): DataFrame = {
    val condition = join
      .map(c => src(c) <=> ref(c))
      .reduce(_ && _)
    src.join(ref, condition, how)
  }

  private def findLatest(src: DataFrame, group: Seq[String], version: Seq[String]): DataFrame = {
    src
      .withColumn("_version", F.struct(version map F.col: _*))
      .withColumn("_version_max", F.max("_version").over(Window.partitionBy(group map F.col: _*)))
      .where(F.col("_version") === F.col("_version_max"))
      .drop("_version", "_version_max")
  }

  private def anyNull(columns: Seq[String]): Column = {
    columns
      .map(c => F.col(c).isNotNull)
      .reduce(_ || _)
  }

  private def notEqualExact(x: Column, y: Column): Column = {
    F.when(F.not(x <=> y), F.to_json(F.array(x, y)))
  }

  private def notEqualApprox(x: Column, y: Column, tol: Column): Column = {
    F.when(
      F.not((x.isNull && y.isNull) || F.coalesce(F.abs(x - y) <= tol, F.lit(false))),
      F.to_json(F.array(x, y))
    )
  }
}

