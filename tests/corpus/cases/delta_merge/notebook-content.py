# Fabric notebook source

# METADATA ********************

# META {
# META   "kernelspec": {
# META     "display_name": "Synapse PySpark",
# META     "language": "Python",
# META     "name": "synapse_pyspark"
# META   },
# META   "language_info": {
# META     "name": "python"
# META   },
# META   "microsoft": {
# META     "language": "python",
# META     "language_group": "synapse_pyspark"
# META   }
# META }

# CELL ********************

import json
from delta.tables import DeltaTable
from pyspark.sql import functions as F

base = spark.read.option('header', True).csv('Files/orders.csv').select('order_id', 'status').withColumn('order_id', F.col('order_id').cast('int'))
base.write.format('delta').mode('overwrite').save('Tables/order_status')

# CELL ********************

updates = spark.createDataFrame([(3, 'paid'), (9, 'paid')], ['order_id', 'status'])
target = DeltaTable.forPath(spark, 'Tables/order_status')
(target.alias('t').merge(updates.alias('u'), 't.order_id = u.order_id').whenMatchedUpdate(set={'status': 'u.status'}).whenNotMatchedInsertAll().execute())

# CELL ********************

spark.sql("MERGE INTO delta.`Tables/order_status` t USING (SELECT 1 AS order_id, 'void' AS status) u ON t.order_id = u.order_id WHEN MATCHED THEN UPDATE SET t.status = u.status")

# CELL ********************

rows = spark.read.format('delta').load('Tables/order_status').orderBy('order_id').collect()
result = {'rows': [[r['order_id'], r['status']] for r in rows], 'versions': DeltaTable.forPath(spark, 'Tables/order_status').history().count()}
notebookutils.notebook.exit(json.dumps(result, sort_keys=True))
