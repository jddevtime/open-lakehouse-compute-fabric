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

%run helpers

# CELL ********************

import json
prices = [10, 20.5, 99.99]

# CELL ********************

%%pyspark
values = [tax(p) for p in prices]

# CELL ********************

result = {'values': values, 'constant': CONSTANT}
notebookutils.notebook.exit(json.dumps(result, sort_keys=True))
