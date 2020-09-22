import pandas as pd
from sqlalchemy import create_engine



def f_SQL_pivot(engine, tablename, values, index = 'CASE_ID', columns = 'DATANUM'):
    query = 'SELECT * FROM inburpt.dbo.' + tablename
    df = pd.read_sql(query,engine)
    
    df = df.pivot(index = index, columns = columns ,values = values)
    
    col = df.columns
    rename = []
    for i in col:
        rename.append(tablename + '_' + '_'.join([str(i) for i in i[::-1]]))


    df.columns = rename
    df.index = df.index.str.strip()

    return df
