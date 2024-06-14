import duckdb


# uat_database.duckdb
conn =  duckdb.connect(':memory:')
# data = conn.execute('select * from social_comments').fetch_df()
tables = conn.execute("SELECT distinct table_name FROM information_schema.tables WHERE table_schema = 'main';").fetchdf()

# print(data.head())
# print(data.shape)

print(tables['table_name'])