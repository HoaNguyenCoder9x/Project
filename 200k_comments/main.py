import os 
import pandas as pd 
from utilities import convert_unidecode, modified_string, count_str
import numpy as np
import duckdb 



    

# ETL PIPELINE

def read_source(file_path: str):
    """
    Read file csv return DF
    
    """

    data = []
    # Read file line by line 
    with open(file_path, 'r', encoding='utf-8') as file:
        for line in file:
            data.append(line.strip().split('\n'))



    df = pd.DataFrame(data)    
    # Rename columns name
    df.columns = ['comments']
    print('Read data from file success, return DF ')
    print(f'Number of rows count in file: {len(data)}/236100')

    return df



def processing(df):
    """
    Add : ascii_comments
    
    """
    # ADD COL: ascii_comments 
    df['ascii_comments'] = df['comments'].apply(convert_unidecode)
    
    # ADD COL: Keywords
    # xử lý columnns keyword
    list_key_words = ['vc','thuy tien','cong vinh', 'bao lu','cong vien thuy tinh','mien trung','tu thien','vo chong']
    kw_cols = []
    for i in list_key_words:
        i = i.replace(' ','_')
        kw_cols.append('kw_' + i)


    for i in range(len(list_key_words)):
        print(f'Xử lý {kw_cols[i]}')
        # break
        # xử lý column keyword
        df[kw_cols[i]] = np.where(df['ascii_comments'].str.contains(list_key_words[i], na = False) , kw_cols[i], '')
    
    # kiểm tra trọng số keyword xuất hiện trên file
    for i in range(len(list_key_words)):
        cols_name = kw_cols[i]
        total_line = len(df)
        kw_line = len(df[df[kw_cols[i]]== kw_cols[i]])
        print(f'Columns {cols_name} have value "{cols_name}" : portition {( kw_line / total_line * 100)}, with {kw_line} LINES IN {total_line}')

    #  xử lý combined keyword rows
    df['combined_row'] = df[kw_cols].apply(lambda row: ' '.join(row.values.astype(str)), axis=1)
    df['combined_row'] = df['combined_row'].str.strip()
    df['combined_row'] = df['combined_row'].str.replace(' ','0')
    df['combined_row'] = df['combined_row'].apply(modified_string)
    
    # xử lý ADD cols Relate points
    df['relate_points'] = df['combined_row'].apply(count_str)
    
    # Filter column
    df = df[df['relate_points'] > 0][['comments','ascii_comments','combined_row', 'relate_points']]
    
    
    return df
    

def write_sink():
    """
    write data from DF to DuckDB
    """


    with duckdb.connect('uat_database.duckdb') as conn:
        conn.execute(f'create table if not exists social_comments as select * from df')
        tables = conn.execute("SELECT * FROM information_schema.tables WHERE table_schema = 'main';").fetchdf()
        print(tables)
        data = conn.execute('select * from social_comments').fetch_df()        
    
    return data




os.chdir('/home/dev_shyn/data/Project/200k_comments')
print(f'Current working dir: {os.getcwd()}')
# /home/dev_shyn/data/Project/200k_comments

# Variables
file_path = '200k_comments.csv'
list_key_words = ['vc','thuy tien','cong vinh', 'bao lu','cong vien thuy tinh','mien trung','tu thien','vo chong']


# Extract
data = read_source(file_path)
# Transform
df = processing(data)
# Load
sink = write_sink()


# OUT TO CONSOLE
print(df.columns)
print(df.shape)
print(sink)


