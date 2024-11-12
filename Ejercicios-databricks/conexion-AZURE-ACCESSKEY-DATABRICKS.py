# Databricks notebook source
# MAGIC %md
# MAGIC Aceso a Azure Data Lake usando access keys

# COMMAND ----------

from pyspark.sql import SparkSession

# Inicializar SparkSession
spark = SparkSession.builder \
    .appName("AzureDataLakeConnection") \
    .getOrCreate()

# Configuración de acceso a Azure Data Lake con Access Key
spark.conf.set("fs.azure.account.key.storagecarmen.dfs.core.windows.net",     "kblOCKU6MCuvirJZd4KzxX4QDxfjbxGi3OCnY1fL5Ypr7IdtFRmIjbw8U0g7dRV7ciB9FkWMmSVJ+AStnO6B/w==")



# COMMAND ----------

# MAGIC %md
# MAGIC comprobamos los ficheros que hay en la carpeta con dbutils

# COMMAND ----------

display(dbutils.fs.ls("abfss://raw@storagecarmen.dfs.core.windows.net"))

# COMMAND ----------

display(spark.read.csv("abfss://raw@storagecarmen.dfs.core.windows.net/linkedinaperfilesmongo.csv"))
