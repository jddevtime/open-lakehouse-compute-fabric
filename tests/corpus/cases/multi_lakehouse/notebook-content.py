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

import csv, json

silver = notebookutils.lakehouse.get('silver')
silver_files = silver['properties']['oneLakeFilesPath']

# CELL ********************

with open('/lakehouse/default/Files/orders.csv') as fh:
    rows = list(csv.DictReader(fh))
paid = [r for r in rows if r['status'] == 'paid' and r['customer_id']]

# CELL ********************

totals = {}
for r in paid:
    totals[r['customer_id']] = round(totals.get(r['customer_id'], 0) + int(r['qty']) * float(r['price']), 2)
notebookutils.fs.mkdirs(silver_files + '/curated')
notebookutils.fs.put(silver_files + '/curated/totals.json', json.dumps(totals, sort_keys=True), True)

# CELL ********************

back = json.loads(notebookutils.fs.head(silver_files + '/curated/totals.json'))
names = sorted(lh['displayName'] for lh in notebookutils.lakehouse.list())
result = {'lakehouses': names, 'default': notebookutils.runtime.context['defaultLakehouseName'], 'totals': back, 'silver_ls': sorted(f.name for f in notebookutils.fs.ls(silver_files + '/curated'))}
notebookutils.notebook.exit(json.dumps(result, sort_keys=True))
