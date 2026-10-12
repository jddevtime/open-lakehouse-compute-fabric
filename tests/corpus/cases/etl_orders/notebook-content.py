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

# MARKDOWN ********************

# # Orders ETL

# PARAMETERS CELL ********************

source_path = "Files/orders.csv"
out_path = "Files/out/orders_clean"
min_qty = 1

# CELL ********************

import json
from pyspark.sql import functions as F

raw = spark.read.option('header', True).csv(source_path)
clean = (raw.where(F.col('customer_id').isNotNull())
         .withColumn('qty', F.col('qty').cast('int'))
         .withColumn('price', F.col('price').cast('double'))
         .withColumn('order_date', F.to_date('order_date'))
         .where(F.col('status') == 'paid')
         .where(F.col('qty') >= min_qty)
         .withColumn('revenue', F.round(F.col('qty') * F.col('price'), 2)))
clean.write.mode('overwrite').parquet(out_path)

# CELL ********************

back = spark.read.parquet(out_path)
agg = back.groupBy('customer_id').agg(F.round(F.sum('revenue'), 2).alias('revenue')).collect()
result = {'rows': back.count(), 'revenue_by_customer': {r['customer_id']: r['revenue'] for r in agg},
          'months': sorted(r[0] for r in back.select(F.date_format('order_date', 'yyyy-MM')).distinct().collect())}
notebookutils.notebook.exit(json.dumps(result, sort_keys=True))
