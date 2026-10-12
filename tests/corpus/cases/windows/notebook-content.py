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
from pyspark.sql import Window, functions as F

sales = spark.read.option('header', True).csv('Files/sales.csv').withColumn('amount', F.col('amount').cast('int'))

# CELL ********************

by_region = Window.partitionBy('region').orderBy(F.col('amount').desc(), 'rep', 'month')
running = Window.partitionBy('region', 'rep').orderBy('month').rowsBetween(Window.unboundedPreceding, Window.currentRow)
ordered = Window.partitionBy('rep').orderBy('month')
w = (sales.withColumn('rn', F.row_number().over(by_region)).withColumn('rank', F.rank().over(Window.partitionBy('region').orderBy(F.col('amount').desc())))
         .withColumn('running', F.sum('amount').over(running)).withColumn('prev', F.lag('amount', 1).over(ordered))
         .withColumn('delta', F.col('amount') - F.col('prev')))

# CELL ********************

top = {r['region']: [r['rep'], r['month'], r['amount']] for r in w.where('rn = 1').collect()}
running_end = {f"{r['rep']}": r['running'] for r in w.where("month = '2026-02'").collect()}
deltas = {r['rep']: r['delta'] for r in w.where("month = '2026-02'").collect()}
ranks = sorted([r['region'], r['rep'], r['month'], r['rank']] for r in w.collect())
result = {'top': top, 'running_end': running_end, 'deltas': deltas, 'ranks': ranks}
notebookutils.notebook.exit(json.dumps(result, sort_keys=True))
