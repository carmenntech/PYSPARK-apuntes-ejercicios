# Databricks notebook source
# MAGIC %md
# MAGIC Aceso a Azure Data Lake usando Shared Access Signature, para generar el sas pulsar sobre los 3 puntos dentro de el contenedor > generar sas

# COMMAND ----------

from pyspark.sql import SparkSession

# Inicializar SparkSession
spark = SparkSession.builder \
    .appName("AzureDataLakeConnection2") \
    .getOrCreate()

# Configuración de acceso a Azure Data Lake con Access Key
spark.conf.set("fs.azure.account.auth.type.storagecarmen.dfs.core.windows.net", "SAS")
spark.conf.set("fs.azure.sas.token.provider.type.storagecarmen.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.sas.FixedSASTokenProvider")
spark.conf.set("fs.azure.sas.fixed.token.storagecarmen.dfs.core.windows.net", "sp=rwl&st=2024-11-12T18:03:50Z&se=2024-11-13T02:03:50Z&spr=https&sv=2022-11-02&sr=c&sig=K93K%2F7E2yEgnoC56%2BKlJ%2BbNlMKWRcwnfDJtbrkrz8mM%3D")


# COMMAND ----------

# MAGIC %md
# MAGIC comprobamos los ficheros que hay en la carpeta con dbutils (marcar poder listar al generar el sas)
# MAGIC

# COMMAND ----------

display(dbutils.fs.ls("abfss://raw@storagecarmen.dfs.core.windows.net"))

# COMMAND ----------

display(spark.read.csv("abfss://raw@storagecarmen.dfs.core.windows.net/linkedinaperfilesmongo.csv"))
