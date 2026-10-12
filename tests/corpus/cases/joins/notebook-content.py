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
from pyspark.sql import functions as F

orders = spark.read.option('header', True).csv('Files/orders.csv').withColumn('qty', F.col('qty').cast('int')).where(F.col('customer_id').isNotNull())
customers = spark.read.option('header', True).csv('Files/customers.csv')

# CELL ********************

inner = orders.join(customers, 'customer_id', 'inner')
left = orders.join(customers, 'customer_id', 'left')
anti = orders.join(customers, 'customer_id', 'left_anti')
bcast = orders.join(F.broadcast(customers), 'customer_id').groupBy('country').agg(F.sum('qty').alias('qty'))

# CELL ********************

a = orders.alias('a')
b = orders.alias('b')
same_sku = a.join(b, (F.col('a.sku') == F.col('b.sku')) & (F.col('a.order_id') < F.col('b.order_id'))).count()

# CELL ********************

result = {
    'inner': sorted(r['order_id'] for r in inner.collect()),
    'left_null_names': sorted(r['order_id'] for r in left.where(F.col('name').isNull()).collect()),
    'anti': sorted(r['customer_id'] for r in anti.select('customer_id').distinct().collect()),
    'qty_by_country': {r['country']: r['qty'] for r in bcast.collect()},
    'same_sku_pairs': same_sku,
}
notebookutils.notebook.exit(json.dumps(result, sort_keys=True))
