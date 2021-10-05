# prepare.py

# imports

import pandas as pd
from datetime import timedelta, datetime
import numpy as np

import warnings
warnings.filterwarnings("ignore")

from acquire import get_store_data

# getting the store data from my stored .csv
df = pd.read_csv('complete_store_data.csv')

def prep_store_data(df)
