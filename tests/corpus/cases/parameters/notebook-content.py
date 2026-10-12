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

run_date = '2026-01-01'
limit = 10
dry_run = True
ratio: float = 0.5
tags = []

# CELL ********************

import json, datetime
d = datetime.date.fromisoformat(run_date)

# CELL ********************

result = {'run_date': run_date, 'weekday': d.strftime('%A'), 'limit': limit, 'limit_type': type(limit).__name__, 'dry_run': dry_run, 'ratio': ratio, 'tags': tags,
          'first': list(range(limit))}
notebookutils.notebook.exit(json.dumps(result, sort_keys=True))
