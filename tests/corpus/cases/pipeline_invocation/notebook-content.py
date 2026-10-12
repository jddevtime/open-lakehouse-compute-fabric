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

batch = 1

# CELL ********************

import json
child = notebookutils.notebook.run('child_load', 120, {'batch': batch})
child_result = json.loads(child)

# CELL ********************

many = notebookutils.notebook.runMultiple({'activities': [{'name': 'load', 'path': 'child_load', 'args': {'batch': 1}}, {'name': 'validate', 'path': 'child_validate', 'dependencies': ['load']}]})

# CELL ********************

result = {'batch': batch, 'child': child_result, 'ok': child_result['rows'] == batch * 10}
notebookutils.notebook.exit(json.dumps(result, sort_keys=True))
