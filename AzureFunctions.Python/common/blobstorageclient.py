"""
Interaction with Azure Blob Storage.
"""

import re
import datetime
import logging
import time
from enum import Enum
from io import TextIOWrapper, BytesIO

import dateutil.parser as dt
from azure.storage.blob import ContainerClient, BlobClient

from .batch import Batch, Status, TYPES, REF_TYPE

STATUS = 'status'
CSV = 'csv'
DEFAULT_ENCODING = 'UTF-8-SIG'

class NamePart(Enum):
    """
    This class gives meaning to the Path Segments of Azure Blob Storage Urls of the files.
    """
    Customer = 0
    Date = 1
    Time = 2
    Type = 3

class BlobStorageClient():
    """
    Class to interact with Azure Blob Storage.
    """

    def __init__(self, container_client: ContainerClient):
        self.__container_client = container_client

    def get_batches(self, subpath: str = None) -> [Batch]:
        """
        Get batches from Azure Blob Storage.
        Valid paths for the csv files in a batch:
        {subpath}/{customer}_{date}_{time}_{type}.csv
        Example:
        input_data/cust1_20171010_1112_type1.csv
        """
        batches = {}
        for blob in self.__container_client.list_blobs(name_starts_with=subpath,
            include='metadata'):
            try:
                # Remove subpath
                full_name = re.sub(f'^{subpath}/', '', blob.name)
                full_name_parts = full_name.rsplit('.', 1)

                # Check file extension
                extension = full_name_parts[1].lower()
                if extension != CSV:
                    # Ignore invalid extensions
                    continue

                # Get customer
                name = full_name_parts[0]
                name_parts = name.rsplit('_')
                customer = name_parts[NamePart.Customer.value]

                # Get timestamp
                file_date = name_parts[NamePart.Date.value]
                file_time = name_parts[NamePart.Time.value]
                timestamp = self.__get_timestamp(file_date, file_time)

                # Check file type
                file_type = name_parts[NamePart.Type.value]
                if file_type not in TYPES:
                    # Ignore invalid types
                    continue

                # Create batch for customer & timestamp and add it to dictionary if needed
                if not batches.get(customer):
                    batches[customer] = {}
                if not batches[customer].get(timestamp):
                    batches[customer][timestamp] = Batch(customer, timestamp)

                # Set blob path for file type
                batches[customer][timestamp].types[file_type] = blob.name

                # Set batch status from the reference's type blob metadata
                if file_type == TYPES[REF_TYPE]:
                    batches[customer][timestamp].status = self.__get_blob_status(blob.metadata)

            except Exception as ex:
                # Ignore invalid blobs
                logging.exception("EXCEPTION while listing blob %s", blob.name, exc_info=ex)

        # Return the batches ready for validation
        ready_batches = []
        for customer_batches in batches.values():
            for batch in customer_batches.values():
                if batch.is_complete() and self.__batch_needs_validation(batch):
                    ready_batches.append(batch)

        return ready_batches

    def save_batch_status(self, batch: Batch):
        """
        Save the status of a batch to the blob metadata of the metadata json file.
        """
        ref_type = batch.types.get(TYPES[REF_TYPE]) if batch else None
        status = {STATUS: batch.status.name} if batch.status is not None else None
        if ref_type:
            self.__container_client.get_blob_client(ref_type).set_blob_metadata(status)

    def download_blob_content(self, path: str) -> TextIOWrapper:
        """
        Download a file as text from Azure Blob Storage.
        """
        blob: BlobClient = self.__container_client.get_blob_client(path)
        stream_downloader = blob.download_blob()
        encoding = stream_downloader.properties.content_settings.content_encoding
        encoding = encoding if encoding else DEFAULT_ENCODING
        contents = stream_downloader.readall()
        stream = TextIOWrapper(BytesIO(contents), encoding=encoding)
        return stream

    def move_blob(self, source_path: str, target_folder: str):
        """
        Move a blob in the container to a target folder.
        """
        target_path = f'{target_folder}/{source_path}'
        target_blob: BlobClient = self.__container_client.get_blob_client(target_path)
        source_url = f'{self.__container_client.url}/{source_path}'
        target_blob.start_copy_from_url(source_url)
        self.__wait_for_copy(target_blob)

        source_blob: BlobClient = self.__container_client.get_blob_client(source_path)
        source_blob.delete_blob()

    def __wait_for_copy(self, blob: BlobClient):
        """
        Wait for the start_copy_from_url method to be completed
        as per: https://github.com/Azure/azure-sdk-for-python/issues/7043
        """
        count = 0
        props = blob.get_blob_properties()
        while props.copy.status == 'pending':
            count = count + 1
            if count > 10:
                raise TimeoutError('Timed out waiting for async copy to complete.')
            time.sleep(5)
            props = blob.get_blob_properties()
        return props

    def __get_blob_status(self, blob_metadata: str) -> str:
        return None if not blob_metadata else blob_metadata.get(STATUS)

    def __batch_needs_validation(self, batch: Batch) -> bool:
        return not batch.status or batch.status == Status.ERROR.name

    def __get_timestamp(self, f_date: str, f_time: str) -> datetime:
        """
        Date looks like this: YYYYMMDD and time look like this: HHMM
        dt.parse requires the timestamp to be like this: "2020-03-27T08:49:30.000Z"
        """
        timestamp = f'{f_date[:4]}-{f_date[4:6]}-{f_date[6:8]}T{f_time[:2]}:{f_time[2:4]}:00.000Z'
        return dt.parse(timestamp)
