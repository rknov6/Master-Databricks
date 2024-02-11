# Databricks notebook source
df = spark.read.options(header = True).csv("abfss://raw@storageadlsdatabricks.dfs.core.windows.net/Countries/countries.csv")
df.display()

# COMMAND ----------


