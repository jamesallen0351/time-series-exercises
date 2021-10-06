import pandas as pd
import numpy as np
import os
from env import host, user, password

###################### Acquire Data ######################

def get_connection(db, user=user, host=host, password=password):
    '''
    This function uses my info from my env file to
    create a connection url to access the Codeup db.
    It takes in a string name of a database as an argument.
    '''
    return f'mysql+pymysql://{user}:{password}@{host}/{db}'
    
    
    
def tsa_item_data():
    '''
    This function reads the tsa_item_demand data from the Codeup db into a df,
    write it to a csv file, and returns the df.
    '''
    # Create SQL query.
    sql_query = """SELECT * from sales, stores, items
    JOIN stores USING (store_id)
    JOIN items USING (item_id);"""


    
    # Read in DataFrame from Codeup db.
    df = pd.read_sql(sql_query, get_connection('tsa_item_demand'))
    
    return df



def get_tsa_data():
    '''
    This function reads in tsa_item_demand data from Codeup database, writes data to
    a csv file if a local file does not exist, and returns a df.
    '''
    if os.path.isfile('tsa_item_demand.csv'):
        
        # If csv file exists, read in data from csv file.
        df = pd.read_csv('tsa_item_demand.csv', index_col=0)
        
    else:
        
        # Read fresh data from db into a DataFrame.
        df = new_titanic_data()
        
        # Write DataFrame to a csv file.
        df.to_csv('tsa_item_demand.csv')
        
    return df

query = 'SELECT stores.*, items.*, sales.sale_date, sales.sale_amount \ FROM sales \ JOIN stores USING (store_id) \ JOIN items USING (item_id);'
