import os

import azure.identity
import azure.storage.filedatalake
import pandas as pd
import numpy as np
import pyarrow
import pyarrow.fs
import pyarrow.parquet
import pyarrow.dataset
import pytest

from .. import core


@pytest.fixture(scope='module')
def credential():
    return azure.identity.DefaultAzureCredential()


@pytest.fixture(scope='module')
def account_name():
    # NB NB: ALL CONTENT IS DELETED AS PART OF TEST RUNNING
    # USE A DEDICATED TESTING ACCOUNT
    return os.environ['AZUREARROWFS_TEST_ACT']


@pytest.fixture(scope='module')
def datalake_service_account(account_name, credential):
    return azure.storage.filedatalake.DataLakeServiceClient(
        f'https://{account_name}.dfs.core.windows.net',
        credential
    )


@pytest.fixture(scope='module')
def _account_handler(datalake_service_account):
    return core.AccountHandler(datalake_service_account)


@pytest.fixture(scope='module')
def account_handler(_account_handler):
    yield _account_handler
    for fs in _account_handler.datalake_service.list_file_systems():
        _account_handler.datalake_service.delete_file_system(fs)


@pytest.fixture(scope='module')
def fs_handler(datalake_service_account):
    if 'testfs' not in [fs.name for fs in datalake_service_account.list_file_systems()]:
        datalake_service_account.create_file_system('testfs')
    return core.FilesystemHandler(datalake_service_account.get_file_system_client('testfs'))


def test_fs_handler_from_account_name_does_not_prefix(account_name, credential):
    handler = core.FilesystemHandler.from_account_name(
        account_name, 'testfs', credential
    )
    assert not handler.prefix_fs


class TestFilesystemHandler:

    def test_list_unknown_directory(self, fs_handler):
        selector = pyarrow.fs.FileSelector('no-such-directory', recursive=True)
        with pytest.raises(NotADirectoryError):
            fs_handler.get_file_info_selector(selector)

    def test_list_unknown_file(self, fs_handler):
        with pytest.raises(FileNotFoundError):
            fs_handler.get_file_info(['no-such-file'])


class TestAccountHandler:

    def test_ls_empty_account(self, account_handler):
        # Getting a fileinfo for the root ("") always succeeds,
        # but using a selector on it yields an empty list
        # This makes sense if you think of the selector as finding the content of the root,
        # but getting a fileinfo explicitly as inspecting the actual root
        listing = account_handler.get_file_info([''])[0]
        assert listing.path == ''
        assert listing.type == pyarrow.fs.FileType.Directory
        listing = account_handler.get_file_info_selector(pyarrow.fs.FileSelector('', recursive=False))
        paths = [fi.path for fi in listing if fi.path != 'testfs']
        assert paths == []

    def test_create_dir_simple(self, account_handler: core.AccountHandler):
        account_handler.create_dir('testfs', False)
        listing = account_handler.get_file_info(['testfs'])[0]
        assert listing.path == 'testfs'
        assert listing.type == pyarrow.fs.FileType.Directory
        listing = account_handler.get_file_info_selector(pyarrow.fs.FileSelector('', recursive=False))[0]
        assert listing.path == 'testfs'
        assert listing.type == pyarrow.fs.FileType.Directory

    def test_create_dir_nested(self, account_handler):
        account_handler.create_dir('testfs/folder/content', True)
        selector = pyarrow.fs.FileSelector('', recursive=True)
        infos = account_handler.get_file_info_selector(selector)
        paths = {info.path for info in infos}
        assert paths == {'testfs', 'testfs/folder', 'testfs/folder/content'}
        assert {pyarrow.fs.FileType.Directory} == {info.type for info in infos}

    def test_writing_file_on_root_fails(self, account_handler):
        with pytest.raises(ValueError):
            account_handler.open_output_stream('root_file')

    def test_delete_dir_fs_account(self, account_handler):
        account_handler.create_dir('testdeletefs', False)
        selector = pyarrow.fs.FileSelector('', recursive=False)
        listing = account_handler.get_file_info_selector(selector)
        assert 'testdeletefs' in [info.path for info in listing]
        account_handler.delete_dir('testdeletefs')
        listing = account_handler.get_file_info_selector(selector)
        assert 'testdeletefs' not in [info.path for info in listing]

    def test_create_dir_nonrecursive_no_fs(self, account_handler):
        with pytest.raises(FileNotFoundError):
            account_handler.create_dir('dont/work', False)

    def test_delete_dir(self, account_handler):
        account_handler.create_dir('testdeletefs2/folder', True)
        selector = pyarrow.fs.FileSelector('testdeletefs2', recursive=False)
        listing = account_handler.get_file_info_selector(selector)
        assert 'testdeletefs2/folder' in [info.path for info in listing]
        account_handler.delete_dir('testdeletefs2/folder')
        listing = account_handler.get_file_info_selector(selector)
        assert 'folder' not in [info.path for info in listing]

    def test_file_io(self, account_handler):
        account_handler.create_dir('writefile/folder', True)
        with account_handler.open_output_stream('writefile/folder/file') as out:
            out.write(b'content')
        with account_handler.open_input_stream('writefile/folder/file') as inp:
            assert inp.read() == b'content'
        file_info = account_handler.get_file_info(['writefile/folder/file'])[0]
        assert file_info.path == 'writefile/folder/file'
        assert file_info.size == 7
        assert file_info.type == pyarrow.fs.FileType.File
        assert file_info.mtime

    def test_delete_dir_contents(self, account_handler):
        account_handler.create_dir('deletecontents/folder', True)
        with account_handler.open_output_stream('deletecontents/file') as out:
            out.write(b'content')
        selector = pyarrow.fs.FileSelector('deletecontents', recursive=True)
        assert len(account_handler.get_file_info_selector(selector)) == 2
        account_handler.delete_dir_contents('deletecontents')
        assert len(account_handler.get_file_info_selector(selector)) == 0

    def test_delete_file(self, account_handler):
        account_handler.create_dir('deletefile', True)
        with pytest.raises(FileNotFoundError):
            account_handler.delete_file('')
        with pytest.raises(IsADirectoryError):
            account_handler.delete_file('deletefile')
        with pytest.raises(FileNotFoundError):
            account_handler.delete_file('nosuchcontainer/file')
        account_handler.create_dir('deletefile', True)
        with pytest.raises(FileNotFoundError):
            account_handler.delete_file('deletefile/file')
        with account_handler.open_output_stream('deletefile/file') as out:
            out.write(b'content')
        selector = pyarrow.fs.FileSelector('deletefile', recursive=True)
        assert len(account_handler.get_file_info_selector(selector)) == 1
        account_handler.delete_file('deletefile/file')
        assert account_handler.get_file_info_selector(selector) == []

    def test_move(self, account_handler):
        account_handler.create_dir('movesrc', False)
        with pytest.raises(ValueError):
            # We don't support moving file systems
            account_handler.move('movesrc', 'movedst')
        account_handler.create_dir('movesrc/folder', True)
        account_handler.create_dir('movedst/move_dir', recursive=True)
        with pytest.raises(ValueError):
            # Target must not not be file system
            account_handler.move('movesrc/folder', 'move_dst')

        with account_handler.open_output_stream('movesrc/move_file') as out:
            out.write(b'content1')
        with account_handler.open_output_stream('movedst/move_dir/dst_file') as out:
            out.write(b'content2')

        with pytest.raises(ValueError):
            # Can't rename file to folder
            account_handler.move('movesrc/move_file', 'movedst/move_dir')
        with pytest.raises(ValueError):
            # Can't move to non-empty dir
            account_handler.move('movesrc/folder', 'movedst/move_dir')
        account_handler.move('movedst/move_dir/dst_file', 'movedst/dst_file')
        account_handler.move('movesrc/folder', 'movedst/move_dir')
        account_handler.move('movesrc/move_file', 'movedst/dst_file')
        with account_handler.open_input_stream('movedst/dst_file') as inp:
            assert inp.read() == b'content1'

    def test_roundtrip_big_file(self, account_handler):
        account_handler.create_dir('bigfile', recursive=False)
        # approx. 40 mb
        df = pd.DataFrame(np.random.normal(size=(1000000, 5)))
        table = pyarrow.Table.from_pandas(df=df)
        with account_handler.open_output_stream('bigfile/bigfile.parq') as bigfile:
            pyarrow.parquet.write_table(table, bigfile)
        del table
        ds = pyarrow.dataset.dataset('bigfile/bigfile.parq', filesystem=account_handler.to_fs())
        df2 = ds.to_table().to_pandas(self_destruct=True)
        assert (df == df2).all().all()

    def test_leading_trailing_slash(self, account_handler):
        account_handler.create_dir('leadingslash/subfolder/ds', recursive=True)
        df = pd.DataFrame(np.random.normal(size=(10, 5)))
        table = pyarrow.Table.from_pandas(df=df)
        with account_handler.open_output_stream('leadingslash/subfolder/ds/part.parq') as out:
            pyarrow.parquet.write_table(table, out)

        pd.read_parquet(
            '/leadingslash/subfolder/ds/part.parq/',
            filesystem=account_handler.to_fs()
        )

    def test_partitioned_parquet(self, account_handler):
        account_handler.create_dir('partitioned/ds', recursive=True)
        fs = account_handler.to_fs()

        table_left = pyarrow.Table.from_pandas(
            pd.DataFrame({'i': list(range(10))}).assign(dir='left')
        )
        table_right = pyarrow.Table.from_pandas(
            pd.DataFrame({'i': list(range(10, 20))}).assign(dir='right')
        )

        with account_handler.open_output_stream('partitioned/ds/dir=left') as out:
            pyarrow.parquet.write_table(table_left, out)
        with account_handler.open_output_stream('partitioned/ds/dir=right') as out:
            pyarrow.parquet.write_table(table_right, out)

        ds = pyarrow.dataset.dataset('partitioned/ds', filesystem=fs, partitioning='hive')
        table_left = ds.to_table(filter=pyarrow.dataset.field('dir') == 'left').to_pandas()
        table_right = ds.to_table(filter=pyarrow.dataset.field('dir') == 'right').to_pandas()

        assert table_left.i.max() == 9
        assert table_right.i.max() == 19
