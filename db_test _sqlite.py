# -*- coding: utf-8 -*-
"""
Created on Wed Jul  8 16:42:28 2020

@author: 
"""

import pandas as pd
import numpy as np
from sqlalchemy import create_engine

frame = pd.DataFrame(np.arange(20).reshape(4,5),
                     columns=['white', 'red', 'blue', 'black', 'green'])

engine = create_engine(r'sqlite:///C:\Users\\Desktop\TempDel\foo.db')

def db_start():
    engine = create_engine(r'sqlite:///C:\Users\\\TempDel\foo.db')
    return engine


def db_write(df=None, table=None, engine=None):
    table = str(table)
    res = df.to_sql(table, engine, index=False, if_exists='replace', chunksize=5000) #append
    assert res == None, 'db writing failed, expected value: none.'
    print('db writing completed.')

def db_read(table=None, engine=None):
    sql = """SELECT * FROM %s LIMIT 20""" %str(table)
    df = pd.read_sql_query(sql, engine)
    return df

#test
db = db_start()
db_write(frame, 'color', db)    
y = db_read('color',engine=db)



pd.read_sql(sql, db)

sql="""SELECT name FROM sqlite_master WHERE type='table' ORDER BY name"""
sql=""".tables"""
sql = 'DROP TABLE IF EXISTS color'
x = db.execute(sql)





