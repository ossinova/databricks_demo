# Databricks notebook source
# MAGIC %run ../00_Config/Functions/transformations

# COMMAND ----------

df_people = spark.table("databricks")

# COMMAND ----------

display(df_people)

# COMMAND ----------

rename_dict = {
    "" : 
}

rename_columns(df = df_people, dict = rename_dict)

# COMMAND ----------

transform_dict = {
    "Full Name" : F.concat_ws(',', [F.col("First"), F.col("Last")])
}

# COMMAND ----------

display(df_transformed)
