# Databricks notebook source
from pyspark.sql.types import StructField,StructType,StringType,IntegerType,DoubleType

# COMMAND ----------

schema = StructType([StructField("COUNTRY_ID",IntegerType(),False),
                    StructField("NAME",StringType(),False),
                    StructField("NATIONALITY",StringType(),False),
                    StructField("COUNTRY_CODE",StringType(),False),
                    StructField("ISO_ALPHA2",StringType(),False),
                    StructField("CAPITAL",StringType(),False),
                    StructField("POPULATION",IntegerType(),False),
                    StructField("AREA_KM2",IntegerType(),False),
                    StructField("REGION_ID",IntegerType(),False),
                    StructField("SUB_REGION_ID",IntegerType(),False),
                    StructField("INTERMEDIATE_REGION_ID",IntegerType(),False),
                    StructField("ORGANIZATION_REGION_ID",IntegerType(),False)
                    ])

# COMMAND ----------

df = spark.read.option("header","True").schema(schema).csv("abfss://raw@storageadlsdatabricks.dfs.core.windows.net/Countries/countries.csv")
df.display()

# COMMAND ----------

df.printSchema()

# COMMAND ----------

display(dbutils.fs.ls("abfss://raw@storageadlsdatabricks.dfs.core.windows.net/Countries"))

# COMMAND ----------

df = spark.read.json("abfss://raw@storageadlsdatabricks.dfs.core.windows.net/Countries/countries_single_line.json")
df.display()

# COMMAND ----------

df = spark.read.option("multiline","True").json("abfss://raw@storageadlsdatabricks.dfs.core.windows.net/Countries/countries_multi_line.json")
df.display()

# COMMAND ----------

df = spark.read.options(sep = "\t",header = True).csv("abfss://raw@storageadlsdatabricks.dfs.core.windows.net/Countries/countries.txt")
df.display()

# COMMAND ----------


