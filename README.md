pyarrowfs-adlgen2
==

pyarrowfs-adlgen2 is an implementation of a pyarrow filesystem for Azure Data Lake Gen2.

It allows you to use pyarrow and pandas to read parquet datasets directly from Azure without
the need to copy files to local storage first.

Compared with [adlfs](https://github.com/fsspec/adlfs/), you may see better performance when reading datasets 
with many files, as pyarrowfs-adlgen2 uses the  datalake gen2 sdk, which has fast directory listing, unlike
the blob sdk used by adlfs.

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

Performance
==

Here is an informal comparison test against adlfs, done against a copy of the 
[NYC taxi dataset](https://learn.microsoft.com/en-us/azure/open-datasets/dataset-taxi-yellow).

The test setup was as follows:

1. Create an Azure Data Lake Gen2 storage account with a container. I clicked through the portal to do this step. Grant
   yourself the Azure Storage Data Owner role on the account.
2. Upload the NYC taxi dataset to the container. You want to do this with `azcopy` or `az cli`, or it's going to take a 
   long time. Here's the command I used, it only took a few seconds:
   `az storage copy -s https://azureopendatastorage.blob.core.windows.net/nyctlc/yellow --recursive -d https://benchpyarrowfs.blob.core.windows.net/taxi/`
3. Set up a venv for the test, and install the dependencies: 
   `python -m venv && source venv/bin/activate && pip install pyarrowfs-adlgen2 pandas pyarrow adlfs azure-identity`
4. Make sure to log in with `az login` and set the correct subscription using `az account set -s playground-sub`

That's the entire test setup. Now we can run some commands against the dataset and time them. Let's see
how long it takes to read the `passengerCount` and `tripDistance` columns for one month of data, 2014/10 using
`pyarrowfs-adlgen2` and the `pyarrow` dataset api:

```shell 
$ time python adlg2_taxi.py 
<class 'pandas.core.frame.DataFrame'>
RangeIndex: 14227692 entries, 0 to 14227691
Data columns (total 2 columns):
 #   Column          Dtype  
---  ------          -----  
 0   passengerCount  int32  
 1   tripDistance    float64
dtypes: float64(1), int32(1)
memory usage: 162.8 MB

real	0m11,000s
user	0m2,018s
sys	0m1,605s
```

Now let's do the same with `adlfs`:

```shell
$ time python adlfs_taxi.py 
<class 'pandas.core.frame.DataFrame'>
RangeIndex: 14227692 entries, 0 to 14227691
Data columns (total 2 columns):
 #   Column          Dtype  
---  ------          -----  
 0   passengerCount  int32  
 1   tripDistance    float64
dtypes: float64(1), int32(1)
memory usage: 162.8 MB

real	0m31,985s
user	0m3,204s
sys	0m2,110s
```

The `pyarrowfs-adlgen2` implementation is about 3 times faster than `adlfs` for this dataset and that's not due to
bandwidth or compute limitations. This reflects my own experience using both professionally as well. I believe that
the difference here is primarily due to the fact that `adlfs` uses the blob storage SDK, which is slow at listing
directories, and that the nyc taxi data set has a lot of files and structure. adlfs is being forced to parse that
to recover the structure, whereas adlgen2 gets it for free from the datalake gen2 SDK.
