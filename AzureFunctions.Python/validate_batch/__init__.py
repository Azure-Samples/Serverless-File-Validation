"""
Queue triggered Azure Function that processes a single batch of files from Azure Blob Storage.
"""

import logging
import os
import json
import jsonpickle

import azure.functions as func
from azure.storage.blob import BlobServiceClient

from ..common import BlobStorageClient
from .batchvalidation import BatchValidation


def main(msg: func.QueueMessage) -> None:
    """
    Entry point for this Azure Function.
    """

    message_content: str = msg.get_body().decode('utf-8')
    logging.info('Python queue trigger function processed a queue item: %s', message_content)

    try:
        # Extract batch info from queue message
        dencoded_batch = jsonpickle.decode(message_content)
        json_value: dict = json.loads(dencoded_batch)

        # Validate batch
        blob_service_client = BlobServiceClient.from_connection_string(os.getenv('DataStorage'))
        container_client = blob_service_client.get_container_client(os.getenv('DataContainer'))
        blob_client: BlobStorageClient = BlobStorageClient(container_client)
        batch_validation: BatchValidation = BatchValidation(blob_client, json_value)
        batch_validation.validate()

        logging.info('Done validating batch')

    except Exception as ex:
        logging.exception('EXCEPTION while processing queue item: %s', message_content, exc_info=ex)
