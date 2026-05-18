# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "891119a3-e68e-4270-b4cb-fa1aed387552",
# META       "default_lakehouse_name": "Demo_LH",
# META       "default_lakehouse_workspace_id": "1325bf95-5569-4a0a-bc01-b357bb103dc8",
# META       "known_lakehouses": [
# META         {
# META           "id": "891119a3-e68e-4270-b4cb-fa1aed387552"
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
