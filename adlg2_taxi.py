#!/usr/bin/env python

import azure.identity
import pyarrow.dataset as dataset
from pyarrowfs_adlgen2 import AccountHandler
fs = AccountHandler.from_account_name(
    'benchpyarrowfs', azure.identity.DefaultAzureCredential()
).to_fs()
ds = dataset.dataset('taxi/yellow.parquet', filesystem=fs, partitioning='hive')
df = ds.filter(
    (dataset.field('puYear') == 2014) & (dataset.field('puMonth') == 10)
).to_table(columns=['passengerCount', 'tripDistance']).to_pandas()
df.info()
