# access-control-benchmark

Running this code would require:

- MySQL to be installed and running,
- an abac, rbac, and pbac database to exist with the correct tables (see ER diagram),
- TPC-H data to be generated and loaded into a MySQL database titled "business",
- engine connections and file paths in the "LOAD DATA" operation in both programs to be changed to fit your sql connection and file locations

policies.py should be ran first if you want to execute queries with access control. 
