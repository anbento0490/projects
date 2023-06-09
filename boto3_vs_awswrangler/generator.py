import pandas as pd
import numpy as np

# Generate mock data
num_rows = 10000
data = {
    'AS_OF_DATE': np.random.choice(pd.date_range(start='2023-07-01', end='2023-07-31'), size=num_rows),
    'COMPANY_CODE': np.random.choice(['ABC', 'XYZ', 'CBA', 'ZYX'], size=num_rows),
    'ACCOUNT_NAME': np.random.choice(['Account A', 'Account B', 'Account C'], size=num_rows),
    'ACCOUNT_NUMBER': np.random.randint(100000, 999999, size=num_rows).astype(str),
    'BALANCE': np.random.uniform(low=0, high=500000, size=num_rows).astype(np.float32),
    'CURRENCY_CODE': np.random.choice(['USD', 'EUR', 'GBP'], size=num_rows),
}

# Create the dataframe
df = pd.DataFrame(data)