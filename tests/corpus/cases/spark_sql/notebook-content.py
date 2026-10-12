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

# PARAMETERS CELL ********************

country = 'DE'

# CELL ********************

import json
from pyspark.sql import functions as F

spark.read.option('header', True).csv('Files/orders.csv').withColumn('qty', F.col('qty').cast('int')).withColumn('price', F.col('price').cast('double')).createOrReplaceTempView('orders')
spark.read.option('header', True).csv('Files/customers.csv').createOrReplaceTempView('customers')

# CELL ********************

%%sql
CREATE OR REPLACE TEMP VIEW paid AS SELECT * FROM orders WHERE status = 'paid' AND customer_id IS NOT NULL

# CELL ********************

by_country = spark.sql(f"""
    WITH j AS (SELECT c.country, o.qty, o.price FROM paid o JOIN customers c USING (customer_id))
    SELECT country, COUNT(*) AS n, ROUND(SUM(qty * price), 2) AS revenue FROM j WHERE country = '{country}' GROUP BY country
""").collect()
ranked = spark.sql('SELECT customer_id, RANK() OVER (ORDER BY qty DESC) AS r FROM paid').orderBy('customer_id', 'r').collect()

# CELL ********************

result = {'by_country': {r['country']: [r['n'], r['revenue']] for r in by_country}, 'ranked': [[r['customer_id'], r['r']] for r in ranked]}
notebookutils.notebook.exit(json.dumps(result, sort_keys=True))
