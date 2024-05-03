#!/usr/bin/env python

import azure.identity
import pandas as pd

storage_options = {
    'account_name': 'benchpyarrowfs',
    'credential': azure.identity.DefaultAzureCredential()
}

df = pd.read_parquet(
    'abfs://taxi/yellow.parquet',
    storage_options=storage_options,
    filters=[('puYear', '==', 2014), ('puMonth', '==', 10)],
    columns=['passengerCount', 'tripDistance']
)
df.info()