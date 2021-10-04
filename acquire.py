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



# Duetschland_Macht
# function to bring in the german power data and convert it to a .csv
def duetschland_macht():
    if os.path.isfile('opsd_germany.csv'):
        return pd.read_csv('opsd_germany.csv')
    df = pd.read_csv('https://raw.githubusercontent.com/jenfly/opsd/master/opsd_germany_daily.csv')
    df.to_csv('Duetschland_Macht.csv')
    return df
