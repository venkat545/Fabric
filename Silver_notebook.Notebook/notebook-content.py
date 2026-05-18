# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "a0336a6c-7d77-4cb9-8173-f8782c0cff7d",
# META       "default_lakehouse_name": "Demo_LH",
# META       "default_lakehouse_workspace_id": "b47707a6-c8f2-45f8-99fb-3259eb3ffa76",
# META       "known_lakehouses": [
# META         {
# META           "id": "a0336a6c-7d77-4cb9-8173-f8782c0cff7d"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

# Welcome to your new notebook
# Type here in the cell editor to add code!
from pyspark.sql import functions as f

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_orders=spark.read.format("Json").load("abfss://133f8506-14d5-4df8-89fc-dc5391fbb7d8@onelake.dfs.fabric.microsoft.com/56475ef2-9188-4ae5-b425-3c2821af0ecd/Files/retail_project/bronze/sales/orders/")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_line_orders=spark.read.format("csv").option("header",True).load("abfss://133f8506-14d5-4df8-89fc-dc5391fbb7d8@onelake.dfs.fabric.microsoft.com/56475ef2-9188-4ae5-b425-3c2821af0ecd/Files/retail_project/bronze/sales/order_lines/")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_line_orders=df_line_orders.withColumn("product_id",f.upper(df_line_orders['product_id']))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_line_orders.show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_line_orders=df_line_orders["order_id"].dropDuplicates()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_line_orders.printSchema()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
