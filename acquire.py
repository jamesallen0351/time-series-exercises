# acquire.py

import pandas as pd
import requests
import os


# creates a df of the selected store data and names it
def store_data(original, name):
    if os.path.isfile(name +'.csv'):
        return pd.read_csv(name + '.csv')
    else: 
        tlist = []
        df = pd.DataFrame()
        response = requests.get(original)
        data = response.json()
        n = data['payload']['max_page']

        for i in range(1,n+1):
            url = original +'?page='+str(i)
            response = requests.get(url)
            data = response.json()
            page_items = data['payload'][name]
            tlist += page_items
            url = original
        df = pd.DataFrame(tlist)
        df.to_csv(name + '.csv',index=False)
        return df

# function to put all the store data together and then convert it to a .csv called 'complete_store_data'
def get_store_data():
    items  = store_data('https://python.zgulde.net/api/v1/items', 'items')
    stores = store_data('https://python.zgulde.net/api/v1/stores', 'stores')
    sales  = store_data('https://python.zgulde.net/api/v1/sales', 'sales')
    item_sales = items.merge(sales, how='left', left_on= items.item_id, right_on=sales.item )
    store_data = item_sales.merge(stores, how='left', left_on='store', right_on=stores.store_id)
    store_data.to_csv('complete_store_data.csv', index=False)
    return store_data


#####
# another way of getting things done
def get_sales_data():
    url = 'https://python.zgulde.net/api/v1/sales'
    response = requests.get(url)

    filename = 'sales.csv'
    if os.path.isfile(filename):
        sales = pd.read_csv(filename, index_col=[0])
    else:
        if response.ok:
            extracted_data = list()
            payload = response.json()['payload']
            max_page = payload['max_page']
            for n in range(max_page):
                extracted_data.extend(payload['sales'])
                try:
                    new_url = url[:25] + payload['next_page']
                    print(new_url)
                    response = requests.get(new_url)
                    payload = response.json()['payload']
                except:
                    pass
                
            sales = pd.DataFrame(extracted_data)
            sales.to_csv(filename)

        else:
            print(response.status_codeus_code)
    return sales

def get_items_data():
    '''
    This function reads in sales data from a url, writes data to
    a csv file if a local file does not exist, and returns a df.
    '''
    if os.path.isfile('items_df.csv'):
        # Reads the csv saved from above, and assigns to the df variable
        df_items = pd.read_csv('items_df.csv', index_col=0)    
        
    else:
        
        base_url = 'https://python.zach.lol'

        api_url = base_url + '/api/v1/'
        response = requests.get(api_url + 'items')
        data = response.json()
    
        # create list from 1st page
        output = data['payload']['items']

        # loop through the pages and add to list
        while data['payload']['next_page'] != None:
    
            response = requests.get(base_url + data['payload']['next_page'])
            data = response.json()
            output.extend(data['payload']['items'])
    
        df_items = pd.DataFrame(output)

        # Cache data
        df_items.to_csv('items_df.csv')
        
    return df_items

############### get the store data

def get_stores_data():
    '''
    This function reads in stores data from a url, writes data to
    a csv file if a local file does not exist, and returns a df.
    '''
    if os.path.isfile('stores_df.csv'):
        
        # If csv file exists read in data from csv file.
        df_stores = pd.read_csv('stores_df.csv', index_col=0)
        
    else:
        
        base_url = 'https://python.zach.lol'
        api_url = base_url + '/api/v1/stores'
        response = requests.get(api_url)
        data = response.json()
        df_stores = pd.DataFrame(data['payload']['stores'])
        
        # Cache data
        df_stores.to_csv('stores_df.csv')
        
    return df_stores

#####

def get_sales_data():
    '''
    This function reads in sales data from a url, writes data to
    a csv file if a local file does not exist, and returns a df.
    '''
    
    if os.path.isfile('sales_df.csv'):
        df_sales = pd.read_csv('sales_df.csv', index_col=0)    
        
    else:
        
        base_url = 'https://python.zach.lol'

        api_url = base_url + '/api/v1/'
        response = requests.get(api_url + 'sales')
        data = response.json()
    
        # create list from 1st page
        output = data['payload']['sales']

        # loop through the pages and add to list
        while data['payload']['next_page'] != None:
    
            response = requests.get(base_url + data['payload']['next_page'])
            data = response.json()
            output.extend(data['payload']['sales'])
    
        df_sales = pd.DataFrame(output)

        # Cache data
        df_sales.to_csv('sales_df.csv')
        
    return df_sales

##### 

def get_stores_combo():
    '''
    This function joins the sales, stores and items dataframes into one
    single data frame and return that df.
    '''
    if os.path.isfile('joined_df.csv'):
        df = pd.read_csv('joined_df.csv', index_col=0) 
    else:
        df_items = get_items_data()
        df_stores = get_stores_data()
        df_sales = get_sales_data()
    
        # left join sales and stores
        df = pd.merge(df_sales, df_stores, left_on='store', right_on='store_id').drop(columns={'store'})
    
        # left join the joined df to the items
        df = pd.merge(df, df_items, left_on='item', right_on='item_id').drop(columns={'item'})

    # Cache data
        df.to_csv('joined_df.csv')
    return df




# Duetschland_Macht
# function to bring in the german power data and convert it to a .csv
def duetschland_macht():
    if os.path.isfile('opsd_germany.csv'):
        return pd.read_csv('opsd_germany.csv')
    df = pd.read_csv('https://raw.githubusercontent.com/jenfly/opsd/master/opsd_germany_daily.csv')
    df.to_csv('Duetschland_Macht.csv')
    return df
