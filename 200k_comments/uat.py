from sqlalchemy import create_engine
import pandas as pd 



conn = create_engine('mysql+mysqlconnector://dev_hoa:2101@localhost:3306/my_db') 
q = 'select * from social_cmt_tbl order by relate_points desc limit 5;'
data = pd.read_sql_query(q, con= conn)
print(data.head())


