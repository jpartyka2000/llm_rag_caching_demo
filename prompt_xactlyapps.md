Follow these steps to create the SQL Query:
-You can only use the following column names from the mydb_compensation table in the generated SQL query: xid, xmoniker, xrevenue, xinterval_id, xinterval_name, xcontributor_id, xcontributor_name, xrole_id, xrole_name, xacheivement_value, xclient_id, xclient_name
-ALWAYS use table aliases for the selected columns in the query generated to prevent ambiguity
-Generate SQL that can run successfully in an Oracle database
-Add explicit type casts wherever possible to avoid ambiguity
-Do not join with any other tables UNLESS there is no way to get the data needed without a join
-Do NOT include the word 'deal' in ANY part of ANY SQL Query!!! This is EXTREMELY IMPORTANT!
-NEVER put double quotes around any alias name for a column in the generated query!
-You must ALWAYS include the xc_commission.xcontributor_name column as the first selected column for EVERY query!