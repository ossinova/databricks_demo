# Databricks notebook source
# ------------ some imports -------------
from delta.tables import DeltaTable
from pyspark.sql import DataFrame
import pyspark.sql.functions as F
from typing import List
from pyspark.sql.session import SparkSession
from pyspark.sql.window import Window

# ------------ import types -------------
from pyspark.sql.types import (
    DateType,
    DoubleType,
    FloatType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
    BooleanType,
    NumericType,
    DecimalType
)

# COMMAND ----------

# DBTITLE 1,Rename columns based on a dictionary
def rename_columns(df: DataFrame, dict: dict) -> DataFrame:
    """
    Selects columns and renames them based on a dictionary mapping

    :df: DataFrame
    :dict: Dictionary

    :return: DataFrame
    """
    return df.select(*[F.col(x).alias(dict[x]) for x in df.columns])

# COMMAND ----------


