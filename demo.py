import delta_sharing

# Point to the profile file. It can be a file on the local file system or a file on a remote storage.
# Most likely "./config.share"
profile_file = "<profile-file-path>"

# Create a SharingClient.
client = delta_sharing.SharingClient(profile_file)

# List all shared tables.
print(client.list_all_tables())

# Create a url to access a shared table.
# A table path is the profile file path following with `#` and the fully qualified name of a table 
# (`<share-name>.<schema-name>.<table-name>`).
# You can get this information by listing tables in the line above
table_url = profile_file + "#<share-name>.<schema-name>.<table-name>"

# # Fetch 10 rows from a table and convert it to a Pandas DataFrame. This can be used to read sample data 
# # from a table that cannot fit in the memory.
table = delta_sharing.load_as_pandas(table_url, limit=10)
print(table)

# Load a table as a Pandas DataFrame. This can be used to process tables that can fit in the memory.
delta_sharing.load_as_pandas(table_url)

# Load a table as a Pandas DataFrame explicitly using Delta Format
delta_sharing.load_as_pandas(table_url, use_delta_format=True)

# Load a table as a Pandas DataFrame, using batch conversion to potentially reduce memory usage.
delta_sharing.load_as_pandas(table_url, convert_in_batches=True)

# Load a table as a Pandas DataFrame explicitly using jsonPredicateHints
hintOnHireDate = '''{
  "op": "equal",
  "children": [
    {"op": "column", "name":"hireDate", "valueType":"date"},
    {"op":"literal","value":"2021-04-29","valueType":"date"}
  ]
}'''
delta_sharing.load_as_pandas(table_url, jsonPredicateHints = hintOnHireDate)

# If the code is running with PySpark, you can use `load_as_spark` to load the table as a Spark DataFrame.
delta_sharing.load_as_spark(table_url)