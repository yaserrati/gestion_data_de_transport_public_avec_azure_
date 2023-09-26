# Databricks notebook source
#Connection configuration
spark.conf.set(
"fs.azure.account.key.yassineharratilake.blob.core.windows.net", "tKNj0rZaMnApWCoA49YBGLqeZ36sEEMjORKATPN+u3SyqWJ5PiBhX81WsJWjivKJSojfyV4RUux/+AStnwClfA=="
)

# COMMAND ----------

#affichage de données de mois 1
spark_df1 = spark.read.format('csv').option('header', True).load("wasbs://datatransport@yassineharratilake.blob.core.windows.net/public_transport_data/raw/public_transport_data_M1.csv")

# COMMAND ----------

#affichage de données de mois 2
spark_df2 = spark.read.format('csv').option('header', True).load("wasbs://datatransport@yassineharratilake.blob.core.windows.net/public_transport_data/raw/public_transport_data_M2.csv")

# COMMAND ----------

#affichage de données de mois 3
spark_df3 = spark.read.format('csv').option('header', True).load("wasbs://datatransport@yassineharratilake.blob.core.windows.net/public_transport_data/raw/public_transport_data_M3.csv")

# COMMAND ----------

#affichage de données de mois 4
spark_df4 = spark.read.format('csv').option('header', True).load("wasbs://datatransport@yassineharratilake.blob.core.windows.net/public_transport_data/raw/public_transport_data_M4.csv")

# COMMAND ----------

#affichage de données de mois 5
spark_df5 = spark.read.format('csv').option('header', True).load("wasbs://datatransport@yassineharratilake.blob.core.windows.net/public_transport_data/raw/public_transport_data_M5.csv")

# COMMAND ----------

#affichage de données de mois 6
spark_df6 = spark.read.format('csv').option('header', True).load("wasbs://datatransport@yassineharratilake.blob.core.windows.net/public_transport_data/raw/public_transport_data_M6.csv")

# COMMAND ----------

spark_df1.show(5)

# COMMAND ----------

spark_df2.show(5)

# COMMAND ----------

spark_df3.show(5)

# COMMAND ----------

spark_df4.show(5)

# COMMAND ----------

spark_df5.show(5)

# COMMAND ----------

spark_df6.show(5)
