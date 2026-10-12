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
from pyspark.sql.types import StringType, IntegerType

@F.udf(returnType=StringType())
def initials(name):
    return None if name is None else name[:1].upper() + '.'

def region_of(country):
    return {'US': 'AMER', 'DE': 'EMEA', 'FR': 'EMEA'}.get(country, 'OTHER')

spark.udf.register('region_of', region_of, StringType())
spark.udf.register('name_len', lambda s: len(s) if s else 0, IntegerType())

# CELL ********************

df = spark.read.option('header', True).csv('Files/customers.csv')
df.createOrReplaceTempView('customers')
a = df.withColumn('initials', initials('name')).orderBy('customer_id').collect()
b = spark.sql('select customer_id, region_of(country) as region, name_len(name) as n from customers order by customer_id').collect()

# CELL ********************

result = {'initials': {r['customer_id']: r['initials'] for r in a}, 'region': {r['customer_id']: r['region'] for r in b}, 'name_len': {r['customer_id']: r['n'] for r in b}}
notebookutils.notebook.exit(json.dumps(result, sort_keys=True))
