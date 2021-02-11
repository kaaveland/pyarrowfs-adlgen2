"""
Adapters to access Azure Data Lake gen2 storage through apache arrow

These are fairly thin wrappers around the azure storage sdk:

https://azuresdkdocs.blob.core.windows.net/$web/python/azure-storage-file-datalake/12.1.1/index.html

Many options in the SDK are unused. For example:
* No interaction with the lease (lock) system happens
* No tags or metadata are set on any SDK objects
* Only defaults are used for ACL/access levels (no public access for created file systems)

Instead of trying to shoehorn functionality like the above into the pyarrow.PyFileSystem API,
it is recommended to use the SDK separately to use this sort of functionality.
"""

import os
import datetime
import io
import typing
import dataclasses

import azure.core.exceptions
import azure.storage.filedatalake
import pyarrow.fs


def _parse_azure_ts(last_modified):
    # Mon, 17 Aug 2020 12:19:35 GMT
    if isinstance(last_modified, str):
        fmt = "%a, %d %b %Y %H:%M:%S %Z"
        return datetime.datetime.strptime(last_modified, fmt)
    else:
        return last_modified


@dataclasses.dataclass
class Timeouts:
    """Timeouts passed to azure.storage.filedatalake operations

    The value of these are provided as the timeout kwarg to the
    corresponding object in azure.storage.filedatalake. Timeout
    units are in seconds."""

    file_client_timeout: typing.Optional[int]
    file_system_timeout: typing.Optional[int]
    datalake_service_timeout: typing.Optional[int]
    directory_client_timeout: typing.Optional[int]

    def __init__(
        self,
        file_client_timeout: typing.Optional[int] = None,
        file_system_timeout: typing.Optional[int] = None,
        datalake_service_timeout: typing.Optional[int] = None,
        directory_client_timeout: typing.Optional[int] = None
    ):
        """
        :param file_client_timeout: timeout in seconds to pass to
            azure.storage.filedatalake.DataLakeFileClient methods
        :param file_system_timeout: timeout in seconds to pass to
            azure.storage.filedatalake.FileSystemClient methods
        :param datalake_service_timeout: timeout in seconds to pass to
            azure.storage.filedatalake.DataLakeServiceClient methods
        :param datalake_service_timeout: timeout in seconds to pass to
            azure.storage.filedatalake.DataLakeDirectoryClient methods
        """

        self.file_client_timeout = file_client_timeout
        self.file_system_timeout = file_system_timeout
        self.datalake_service_timeout = datalake_service_timeout
        self.directory_client_timeout = directory_client_timeout


DEFAULT_TIMEOUTS = Timeouts()


def document_timeout(target, timeout_used):
    def wrap_method(method):
        method.__doc__ = f"""Return result of azure.storage.filedatalake.{target.__qualname__}

        Affected by self.timeouts.{timeout_used}"""
        return method
    return wrap_method


class DatalakeGen2File(io.IOBase):
    """Write and read files from Azure Data Lake gen2.

    Normally, you would not use this directly but get an instance from either
    FilesystemHandler.open_* or AccountHandler.open_* methods.
    """

    DEFAULT_BLOCK_SIZE = 5 * 2 ** 20

    def __init__(self, file_client: azure.storage.filedatalake.DataLakeFileClient,
                 mode='rb', block_size='default', timeouts=DEFAULT_TIMEOUTS):
        super(DatalakeGen2File, self).__init__()
        self.timeouts = timeouts
        self.file_client = file_client
        self.mode = mode
        self.block_size = self.DEFAULT_BLOCK_SIZE if block_size == 'default' else block_size
        self.loc = 0
        self.buffer = io.BytesIO()
        self.offset = None

        if mode not in {'ab', 'rb', 'wb'}:
            raise ValueError(f"File mode not supported: {mode}")

        if self.mode == 'wb':
            self.file_client.create_file()

    @document_timeout(
        azure.storage.filedatalake.DataLakeFileClient.get_file_properties,
        "file_client_timeout")
    def get_file_properties(self):
        return self.file_client.get_file_properties(
            timeout=self.timeouts.file_client_timeout
        )

    @document_timeout(
        azure.storage.filedatalake.DataLakeFileClient.append_data,
        "file_client_timeout")
    def append_data(self, data, offset, length):

        # timeout is undocumented here, but is still being used in the sdk code
        return self.file_client.append_data(
            data, offset, length, timeout=self.timeouts.file_client_timeout
        )

    @document_timeout(
        azure.storage.filedatalake.DataLakeFileClient.flush_data,
        "file_client_timeout")
    def flush_data(self, offset):

        # timeout is undocumented here, but is still being used in the sdk code
        return self.file_client.flush_data(
            offset, timeout=self.timeouts.file_client_timeout
        )

    @document_timeout(
        azure.storage.filedatalake.DataLakeFileClient.download_file,
        "file_client_timeout")
    def download_file(self, loc, length):

        return self.file_client.download_file(
            loc, length, timeout=self.timeouts.file_client_timeout
        )

    def tell(self):
        return self.loc

    def seek(self, loc, whence=0):
        if not self.mode == "rb":
            raise ValueError("Seek only available in read mode")
        if not 0 <= whence <= 2:
            raise ValueError(f'Invalid whence {whence}, should be 0, 1 or 2')
        new_loc = [
            loc,
            self.loc + loc,
            self.get_file_properties().size + loc
        ][whence]
        if new_loc < 0:
            raise ValueError("Seek before start of file")
        self.loc = new_loc
        return new_loc

    def write(self, data):
        if self.mode not in {"wb", "ab"}:
            raise ValueError("File not in write mode")
        if self.closed:
            raise ValueError("Attempted I/O on closed file")
        out = self.buffer.write(data)
        self.loc += out
        if self.buffer.tell() >= self.block_size:
            self.flush()
        return out

    def writeable(self):
        return self.mode in {'wb', 'ab'}

    def flush(self):
        if self.closed:
            raise ValueError("Flush on closed file")
        if self.mode not in {'wb', 'ab'}:
            return

        if self.offset is None:
            if self.mode == 'ab':
                self.offset = self.get_file_properties().size
            else:
                self.offset = 0

        self.append_data(self.buffer.getvalue(), self.offset, self.buffer.tell())
        self.offset += self.buffer.tell()
        self.flush_data(self.offset)
        self.buffer = io.BytesIO()

    def read(self, length=-1):
        if self.mode != 'rb':
            raise ValueError("File not in read mode")
        if self.closed:
            raise ValueError('I/O on closed file')
        if length < 0:
            length = self.get_file_properties().size - self.loc
        if length == 0:
            return b''

        return self.download_file(self.loc, length).readall()


class FilesystemHandler(pyarrow.fs.FileSystemHandler):
    """
    Handler for a single file system within an azure storage account.

    Use this if you do not have access to the account itself, f. ex. if you have a SAS token
    that has access only to a single file system.
    """

    def __init__(
            self,
            file_system_client: azure.storage.filedatalake.FileSystemClient,
            prefix_fs=False,
            timeouts=DEFAULT_TIMEOUTS
    ):
        """
        :param file_system_client:
        :param prefix_fs: If True, prefix the name of the file system to all generated paths
        :param timeouts: :class:`Timeouts` for datalake gen2 operations
        :type file_system_client: azure.storage.filedatalake.FileSystemClient
        :type prefix_fs: bool

        https://azuresdkdocs.blob.core.windows.net/$web/python/azure-storage-file-datalake/12.1.1/azure.storage.filedatalake.html#azure.storage.filedatalake.FileSystemClient
        """
        super().__init__()
        self.prefix_fs = prefix_fs
        self.file_system_client = file_system_client
        self.timeouts = timeouts

    def _prefix(self, path):
        if self.prefix_fs and path:
            return f'{self.file_system_client.file_system_name}/{path}'
        elif self.prefix_fs and not path:
            return self.file_system_client.file_system_name
        else:
            return path

    @document_timeout(
        azure.storage.filedatalake.FileSystemClient.get_paths,
        "file_system_timeout")
    def get_paths(self, path, recursive=False):
        return self.file_system_client.get_paths(
            path, recursive=recursive, timeout=self.timeouts.file_system_timeout
        )

    @document_timeout(
        azure.storage.filedatalake.DataLakeFileClient.rename_file,
        "file_client_timeout")
    def rename_file(self, src_path, dest_path):
        return self.file_system_client.get_file_client(src_path).rename_file(
            dest_path, timeout=self.timeouts.file_client_timeout
        )

    @document_timeout(
        azure.storage.filedatalake.DataLakeDirectoryClient.rename_directory,
        "directory_client_timeout")
    def rename_directory(self, src_path, dest_path):
        return self.file_system_client.get_directory_client(src_path).rename_directory(
            dest_path, timeout=self.timeouts.directory_client_timeout
        )

    @document_timeout(
        azure.storage.filedatalake.FileSystemClient.create_directory,
        "file_system_timeout")
    def create_directory(self, path):
        return self.file_system_client.create_directory(
            path, timeout=self.timeouts.file_system_timeout)

    @document_timeout(
        azure.storage.filedatalake.FileSystemClient.delete_directory,
        "file_system_timeout")
    def delete_directory(self, path):
        return self.file_system_client.delete_directory(
            path, timeout=self.timeouts.file_system_timeout
        )

    @document_timeout(
        azure.storage.filedatalake.DataLakeFileClient.delete_file,
        "file_client_timeout")
    def delete_file_(self, path):
        return self.file_system_client.get_file_client(path).delete_file(
            timeout=self.timeouts.file_client_timeout
        )

    @classmethod
    def from_account_name(
            cls,
            account_name,
            file_system_name,
            credential=None,
            timeouts=DEFAULT_TIMEOUTS
    ):
        """
        Create from storage account name, file system name and credential

        :param account_name:
        :param file_system_name:
        :param credential: Any valid valid value to pass as credential to
            azure.storage.filedatalake.FileSystemClient
        :param timeouts: :class:`Timeouts` for datalake gen2 operations
        :type credential: str for SAS tokens, None for public access,
            any credential from azure.identity
        :return: FilesystemHandler
        """
        client = azure.storage.filedatalake.FileSystemClient(
            f'https://{account_name}.dfs.core.windows.net',
            file_system_name,
            credential=credential
        )
        return cls(client, timeouts)

    def __eq__(self, other):
        if isinstance(other, FilesystemHandler):
            return (
                self.file_system_client == other.file_system_client
                and self.timeouts == other.timeouts)
        return NotImplemented

    def __neq__(self, other):
        if isinstance(other, FilesystemHandler):
            return (
                self.file_system_client != other.file_system_client
                or self.timeouts != other.timeouts)
        return NotImplemented

    def get_type_name(self):
        # azure blob file system
        return f"abfs+{self.file_system_client.account_name}/{self.file_system_client.file_system_name}"

    def normalize_path(self, path: str):
        return path.lstrip('/').rstrip('/')

    def _create_file_info(
            self,
            path_properties: azure.storage.filedatalake._models.PathProperties
    ):
        if path_properties.is_directory:
            path_type = pyarrow.fs.FileType.Directory
        else:
            path_type = pyarrow.fs.FileType.File
        return pyarrow.fs.FileInfo(
            self._prefix(path_properties.name),
            path_type,
            size=path_properties.content_length,
            mtime=_parse_azure_ts(path_properties.last_modified)
        )

    def _verify_is_dir(self, path: str):
        if path in {'', '/'}:
            # The root always exists
            return
        try:
            parent = os.path.dirname(path)
            path_property_result = self.get_paths(parent, recursive=False)
            for path_properties in path_property_result:
                if path_properties.name == path:
                    if not path_properties.is_directory:
                        raise NotADirectoryError(self._prefix(path))
                    return
            raise NotADirectoryError(self._prefix(path))
        except azure.storage.filedatalake._models.StorageErrorException as e:
            if e.status_code == 404:
                raise FileNotFoundError(self._prefix(path))
            else:
                raise

    def _get_file_info(self, path):
        if not path.lstrip('/'):
            return pyarrow.fs.FileInfo(
                self.file_system_client.file_system_name if self.prefix_fs else '',
                pyarrow.fs.FileType.Directory
            )
        parent = os.path.dirname(path)
        listing = self.get_paths(parent, recursive=False)
        for path_properties in listing:
            if path_properties.name == path:
                return self._create_file_info(path_properties)
        raise FileNotFoundError(self._prefix(path))

    def get_file_info(self, paths: [str]):
        return [
            self._get_file_info(self.normalize_path(path)) for path in paths
        ]

    def get_file_info_selector(self, selector: pyarrow.fs.FileSelector):
        try:
            self._verify_is_dir(self.normalize_path(selector.base_dir))
        except FileNotFoundError:
            if selector.allow_not_found:
                return []
            else:
                raise

        listing = self.get_paths(
            self.normalize_path(selector.base_dir),
            recursive=selector.recursive
        )

        return [
            self._create_file_info(path_properties)
            for path_properties in listing
        ]

    def create_dir(self, path, recursive):
        path = self.normalize_path(path)
        if recursive:
            self.create_directory(path)
        else:
            parent = os.path.dirname(path)
            self._verify_is_dir(parent)
            self.create_directory(path)

    def delete_dir(self, path):
        path = self.normalize_path(path)
        self._verify_is_dir(path)
        self.delete_directory(path)

    def delete_dir_contents(self, path, accept_root_dir=False):
        path = self.normalize_path(path)
        self._verify_is_dir(path)
        if not accept_root_dir and path in {'', '/'}:
            raise ValueError('Attempt to delete root dir with accept_root_dir=False')
        for path_properties in self.get_paths(path, recursive=False):
            if path_properties.is_directory:
                self.delete_directory(path_properties.name)
            else:
                self.delete_file_(path_properties.name)

    def delete_root_dir_contents(self, path):
        self.delete_dir_contents(path=None)

    def delete_file(self, path):
        path = self.normalize_path(path)
        file_info: pyarrow.fs.FileInfo = self.get_file_info([path])[0]
        if not file_info.is_file:
            raise IsADirectoryError(self._prefix(path))
        self.delete_file_(path)

    def move(self, src, dest):
        # This is a simple rename. Caveat: the dest path is not relative to the file_system,
        # the azure-sdk expects the file system to be prefixed to the new path.
        src = self.normalize_path(src)
        dest = self.normalize_path(dest)
        src_info = self.get_file_info([src])[0]
        if src_info.type == pyarrow.fs.FileType.Directory:
            self.rename_directory(src, self.file_system_client.file_system_name + '/' + dest)
        else:
            self.rename_file(src, self.file_system_client.file_system_name + '/' + dest)

    def copy_file(self, src, dest):
        src = self.normalize_path(src)
        dest = self.normalize_path(dest)
        try:
            info = self.get_file_info([dest])[0]
            if info.type == pyarrow.fs.FileType.Directory:
                raise IsADirectoryError(self._prefix(dest))
        except FileNotFoundError as ignore: # noqa
            pass

        # There is actually no API call to do this, so it must be implemented with read/write
        with self.open_input_stream(src) as source:
            with self.open_output_stream(dest) as out:
                out.write(source.read())

    def open_input_stream(self, path):
        path = self.normalize_path(path)
        self._verify_is_file(path)
        fc = self.file_system_client.get_file_client(path)
        return pyarrow.PythonFile(DatalakeGen2File(fc, mode='rb', timeouts=self.timeouts))

    def open_input_file(self, path):
        path = self.normalize_path(path)
        self._verify_is_file(path)
        fc = self.file_system_client.get_file_client(path)
        return pyarrow.PythonFile(DatalakeGen2File(fc, mode='rb', timeouts=self.timeouts))

    def open_output_stream(self, path):
        path = self.normalize_path(path)
        fc = self.file_system_client.get_file_client(path)
        return pyarrow.PythonFile(DatalakeGen2File(fc, mode='wb', timeouts=self.timeouts))

    def open_append_stream(self, path):
        path = self.normalize_path(path)
        fc = self.file_system_client.get_file_client(path)
        return pyarrow.PythonFile(DatalakeGen2File(fc, mode='ab', timeouts=self.timeouts))

    def _verify_is_file(self, path):
        info = self.get_file_info([path])[0]
        if not info.is_file:
            raise FileNotFoundError(self._prefix(path))

    def to_fs(self):
        return pyarrow.fs.PyFileSystem(self)


class AccountHandler(pyarrow.fs.FileSystemHandler):
    """Handler for a single azure storage account.

    Use this to to access an Azure Storage account with hierarchial namespace enabled.
    """

    def __init__(
            self,
            datalake_service: azure.storage.filedatalake.DataLakeServiceClient,
            timeouts=DEFAULT_TIMEOUTS,
            fs_handler_cls=FilesystemHandler
    ):
        """
        :param datalake_service: data lake account service
        :param timeouts: :class:`Timeouts` for datalake gen2 operations
        :param fs_handler_cls: How to create FilesystemHandlers for interacting with
            individual file systems in this account
        :type datalake_service: azure.storage.filedatalake.DataLakeServiceClient

        https://azuresdkdocs.blob.core.windows.net/$web/python/azure-storage-file-datalake/12.1.1/azure.storage.filedatalake.html#azure.storage.filedatalake.DataLakeServiceClient
        """
        super().__init__()
        self.datalake_service = datalake_service
        self.file_system_handlers = {}
        self.timeouts = timeouts
        self.fs_handler_cls = fs_handler_cls

    @classmethod
    def from_account_name(
            cls,
            account_name,
            credential=None,
            timeouts=DEFAULT_TIMEOUTS,
            fs_handler_cls=FilesystemHandler
    ):
        """
        Create from storage account name and credential

        :param account_name:
        :param credential: Any valid valid value to pass as credential to
            azure.storage.filedatalake.FileSystemClient
        :param timeouts: :class:`Timeouts` for datalake gen2 operations
        :param fs_handler_cls: How to create FilesystemHandlers for interacting with
            individual file systems in this account
        :type credential: str for SAS tokens, None for public access, any credential
            from azure.identity
        :return: pyarrow.fs.FileSystemHandler"""
        datalake_service = azure.storage.filedatalake.DataLakeServiceClient(
            f'https://{account_name}.dfs.core.windows.net',
            credential
        )
        return cls(datalake_service, timeouts)

    def __eq__(self, other):
        if isinstance(other, AccountHandler):
            return (
                self.datalake_service == other.datalake_service
                and self.timeouts == other.timeouts)
        return NotImplemented

    def __neq__(self, other):
        if isinstance(other, AccountHandler):
            return (
                self.datalake_service != other.datalake_service
                or self.timeouts != other.timeouts)
        return NotImplemented

    def get_type_name(self):
        # azure blob file system
        return f'abfs+{self.datalake_service.account_name}'

    def normalize_path(self, path):
        return path.lstrip('/').rstrip('/')

    def _split_path(self, path):
        path = self.normalize_path(path)
        if '/' not in path:
            return path, ''
        fs_name, *rest = path.split('/')
        path = '/'.join(rest)
        if path.endswith('/'):
            raise ValueError(f'{path} is an illegal path (may not end with /)')
        return fs_name, path

    def _fs(self, fs_name):
        if fs_name in self.file_system_handlers:
            return self.file_system_handlers[fs_name]
        else:
            new_fs_handler = self.fs_handler_cls(
                self.datalake_service.get_file_system_client(fs_name),
                prefix_fs=True,
                timeouts=self.timeouts
            )
            return self.file_system_handlers.setdefault(fs_name, new_fs_handler)

    def _get_file_info(self, path):
        fs_name, path = self._split_path(path)
        if not fs_name:
            return pyarrow.fs.FileInfo(
                '',
                pyarrow.fs.FileType.Directory
            )
        return self._fs(fs_name)._get_file_info(path)

    def get_file_info(self, paths):
        return [self._get_file_info(path) for path in paths]

    @document_timeout(
        azure.storage.filedatalake.DataLakeServiceClient.list_file_systems,
        "datalake_service_timeout")
    def list_file_systems(self):
        return self.datalake_service.list_file_systems(
            timeout=self.timeouts.datalake_service_timeout)

    @document_timeout(
        azure.storage.filedatalake.DataLakeServiceClient.create_file_system,
        "datalake_service_timeout")
    def create_file_system(self, name):
        return self.datalake_service.create_file_system(
            name, timeout=self.timeouts.datalake_service_timeout
        )

    @document_timeout(
        azure.storage.filedatalake.DataLakeServiceClient.delete_file_system,
        "datalake_service_timeout")
    def delete_file_system(self, name):
        return self.datalake_service.delete_file_system(
            name, timeout=self.timeouts.datalake_service_timeout
        )

    def get_file_info_selector(self, selector: pyarrow.fs.FileSelector):
        fs_name, path = self._split_path(selector.base_dir)
        if not fs_name:
            file_system_data = [
                pyarrow.fs.FileInfo(fs.name, pyarrow.fs.FileType.Directory, mtime=fs.last_modified)
                for fs in self.list_file_systems()
            ]
            if selector.recursive:
                for fs in self.list_file_systems():
                    file_system_data.extend(self._fs(fs.name).get_file_info_selector(selector))
            return file_system_data
        else:
            sub_selector = pyarrow.fs.FileSelector(
                path, allow_not_found=selector.allow_not_found, recursive=selector.recursive
            )
            return self._fs(fs_name).get_file_info_selector(sub_selector)

    def create_dir(self, path, recursive):
        fs_name, path = self._split_path(path)

        if recursive or not path:
            try:
                fs_client = self.create_file_system(fs_name)
                self.file_system_handlers[fs_name] = self.fs_handler_cls(
                    fs_client, prefix_fs=True, timeouts=self.timeouts)
            except azure.core.exceptions.ResourceExistsError:
                pass
        if path:
            if fs_name not in [fs.name for fs in self.list_file_systems()]:
                raise FileNotFoundError(fs_name)
            self._fs(fs_name).create_dir(path, recursive)

    def delete_dir(self, path):
        fs_name, path = self._split_path(path)
        if not path:
            self.delete_file_system(fs_name)
        else:
            self._fs(fs_name).delete_dir(path)

    def delete_dir_contents(self, path, accept_root_dir=False):
        fs_name, path = self._split_path(path)
        if not fs_name:
            if accept_root_dir:
                for fs in self.list_file_systems():
                    self.delete_file_system(fs.name)
            else:
                raise ValueError('Attempt to remove root dir with accept_root_dir=False')
        else:
            # In _our_ context, root dir can not be within the child file system
            self._fs(fs_name).delete_dir_contents(path, accept_root_dir=True)

    def delete_root_dir_contents(self, path):
        self.delete_dir_contents("")

    def delete_file(self, path):
        fs_name, path = self._split_path(path)
        if not fs_name:
            raise FileNotFoundError()
        elif not path:
            raise IsADirectoryError(fs_name)
        else:
            if fs_name not in [fs.name for fs in self.list_file_systems()]:
                raise FileNotFoundError(fs_name)
            self._fs(fs_name).delete_file(path)

    def move(self, src, dest):
        src_fs, src_path = self._split_path(src)
        dst_fs, dst_path = self._split_path(dest)

        if not src_path:
            raise ValueError(f'Unsupported operation: moving fs {src_fs}')
        if not dst_path:
            raise ValueError(f'Unsupported operation: new name is file system {dst_fs}')

        # Assume source exists, let caller deal with error
        fi = self._fs(src_fs).get_file_info([src_path])[0]

        try:
            dest_fi = self.get_file_info([dest])[0]
            if dest_fi.type == pyarrow.fs.FileType.Directory:
                # Allow only if it is empty
                selector = pyarrow.fs.FileSelector(dest, recursive=False)
                if self.get_file_info_selector(selector):
                    raise ValueError(f'{dest} is non-empty directory')
            if fi.type != dest_fi.type:
                raise ValueError(f'src {src} is {fi.type}, but dest {dest} is {dest_fi.type}')
        except FileNotFoundError:
            pass

        if fi.is_file:
            self._fs(src_fs).rename_file(src_path, dest)
        else:
            self._fs(src_fs).rename_directory(src_path, dest)

    def copy_file(self, src, dest):
        try:
            dest_fi = self._get_file_info(dest)
            if dest_fi.type == pyarrow.fs.FileType.Directory:
                raise IsADirectoryError(dest)
        except FileNotFoundError:
            pass

        with self.open_input_stream(src) as read_from:
            with self.open_output_stream(dest) as write_to:
                write_to.write(read_from.read())

    def _require_path(self, path):
        if not path:
            raise ValueError('Files can not exist on the root account level')

    def open_input_stream(self, path):
        fs_name, path = self._split_path(path)
        self._require_path(path)
        return self._fs(fs_name).open_input_stream(path)

    def open_input_file(self, path):
        fs_name, path = self._split_path(path)
        self._require_path(path)
        return self._fs(fs_name).open_input_file(path)

    def open_output_stream(self, path):
        fs_name, path = self._split_path(path)
        self._require_path(path)
        return self._fs(fs_name).open_output_stream(path)

    def open_append_stream(self, path):
        fs_name, path = self._split_path(path)
        self._require_path(path)
        return self._fs(fs_name).open_append_stream(path)

    def to_fs(self):
        return pyarrow.fs.PyFileSystem(self)
