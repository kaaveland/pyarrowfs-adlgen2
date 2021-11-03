pyarrowfs-adlgen2
==

pyarrowfs-adlgen2 is an implementation of a pyarrow filesystem for Azure Data Lake Gen2.

It allows you to use pyarrow and pandas to read parquet datasets directly from Azure without the need to copy files to local storage first.

Installation
--

`pip install pyarrowfs-adlgen2`

Reading datasets
--

Example usage with pandas dataframe:

```python
import azure.identity
import pandas as pd
import pyarrow.fs
import pyarrowfs_adlgen2

handler = pyarrowfs_adlgen2.AccountHandler.from_account_name(
    'YOUR_ACCOUNT_NAME', azure.identity.DefaultAzureCredential())
fs = pyarrow.fs.PyFileSystem(handler)
df = pd.read_parquet('container/dataset.parq', filesystem=fs)
```

Example usage with arrow tables:

```python
import azure.identity
import pyarrow.dataset
import pyarrow.fs
import pyarrowfs_adlgen2

handler = pyarrowfs_adlgen2.AccountHandler.from_account_name(
    'YOUR_ACCOUNT_NAME', azure.identity.DefaultAzureCredential())
fs = pyarrow.fs.PyFileSystem(handler)
ds = pyarrow.dataset.dataset('container/dataset.parq', filesystem=fs)
table = ds.to_table()
```

Configuring timeouts
--

Timeouts are passed to azure-storage-file-datalake SDK methods. The timeout unit is in seconds.

```python
import azure.identity
import pyarrowfs_adlgen2

handler = pyarrowfs_adlgen2.AccountHandler.from_account_name(
    'YOUR_ACCOUNT_NAME',
    azure.identity.DefaultAzureCredential(),
    timeouts=pyarrowfs_adlgen2.Timeouts(file_system_timeout=10)
)
# or mutate it:
handler.timeouts.file_client_timeout = 20
```

Writing datasets
--

With pyarrow version 3 or greater, you can write datasets from arrow tables:

```python
import pyarrow as pa
import pyarrow.dataset

pyarrow.dataset.write_dataset(
    table,
    'name.pq',
    format='parquet',
    partitioning=pyarrow.dataset.partitioning(
        schema=pyarrow.schema([('year', pa.int32())]), flavor='hive'
    ),
    filesystem=pyarrow.fs.PyFileSystem(handler)
)
```

With earlier versions, files must be opened/written one at a time:

As of pyarrow version 1.0.1, `pyarrow.parquet.ParquetWriter` does not support `pyarrow.fs.PyFileSystem`, but data can be written to open files:

```python
with fs.open_output_stream('container/out.parq') as out:
    df.to_parquet(out)
```

Or with arrow tables:

```python
import pyarrow.parquet

with fs.open_output_stream('container/out.parq') as out:
    pyarrow.parquet.write_table(table, out)
```

Accessing only a single container/file-system
--

If you do not want, or can't access the whole storage account as a single filesystem, you can use `pyarrowfs_adlgen2.FilesystemHandler` to view a single file system within an account:

```python
import azure.identity
import pyarrowfs_adlgen2

handler = pyarrowfs_adlgen2.FilesystemHandler.from_account_name(
   "STORAGE_ACCOUNT", "FS_NAME", azure.identity.DefaultAzureCredential())
```

All access is done through the file system within the storage account.

Set http headers for files for pyarrow >= 5
--

You can set headers for any output files by using the `metadata` argument to `handler.open_output_stream`:

```python
import pyarrowfs_adlgen2

fs = pyarrowfs_adlgen2.AccountHandler.from_account_name("theaccount").to_fs()
metadata = {"content_type": "application/json"}
with fs.open_output_stream("container/data.json", metadata) as out:
    out.write("{}")
```

Note that the spelling is different than you might expect! For a list of valid keys, see
[ContentSettings](https://docs.microsoft.com/en-us/python/api/azure-storage-file-datalake/azure.storage.filedatalake.contentsettings?view=azure-python).

You can do this for pyarrow >= 5 when using `pyarrow.fs.PyFileSystem`, and for any pyarrow if using the handlers
from pyarrowfs_adlgen2 directly.


Running tests
--

To run the integration tests, you need:

- Azure Storage Account V2 with hierarchial namespace enabled (Data Lake gen2 account)
- To configure azure login (f. ex. use `$ az login` or set up environment variables, see ` azure.identity.DefaultAzureCredential`)
- Install pytest, f. ex. `pip install pytest`

**NB! All data in the storage account is deleted during testing, USE AN EMPTY ACCOUNT**

```
AZUREARROWFS_TEST_ACT=thestorageaccount pytest
```
