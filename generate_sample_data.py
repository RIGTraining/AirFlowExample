# generate_sample_data.py
import pandas as pd
import numpy as np
from datetime import datetime, timedelta

# Generate sample sales data
np.random.seed(42)

n_records = 1000
dates = [datetime.now() - timedelta(days=x) for x in range(n_records)]
products = ['Product_A', 'Product_B', 'Product_C', 'Product_D']
regions = ['North', 'South', 'East', 'West']

df = pd.DataFrame({
    'id': range(1, n_records + 1),
    'date': dates,
    'product': np.random.choice(products, n_records),
    'region': np.random.choice(regions, n_records),
    'quantity': np.random.randint(1, 100, n_records),
    'price': np.random.uniform(10, 500, n_records).round(2),
    'customer_id': np.random.randint(1000, 9999, n_records)
})

# Add some missing values for testing
df.loc[10:20, 'quantity'] = np.nan
df.loc[50:55, 'price'] = np.nan
df.loc[100:105, 'date'] = np.nan
df.loc[200:202, 'customer_id'] = np.nan

# Add some duplicates
df = pd.concat([df, df.iloc[150:155]], ignore_index=True)

# Save to CSV
df.to_csv('source_data.csv', index=False)
print(f"Sample data generated: {len(df)} rows saved to source_data.csv")