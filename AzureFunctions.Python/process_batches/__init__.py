"""
Time triggered Azure Function that processes batches of files from Azure Blob Storage for their
validation.
"""

import datetime
import logging
import os
from typing import List

import azure.functions as func
from azure.storage.blob import BlobServiceClient
from ..common import BlobStorageClient, Batch, Status, TYPES

def main(mytimer: func.TimerRequest, myqueue: func.Out[List[str]]) -> None:
    """
    Entry point for this Azure Function.
    """

    utc_timestamp = datetime.datetime.utcnow().replace(
        tzinfo=datetime.timezone.utc).isoformat()

    if mytimer.past_due:
        logging.info('The timer is past due!')

    logging.info('Python timer trigger function ran at %s', utc_timestamp)

    try:
        # Get all batches ready to be validated from the storage
        blob_service_client = BlobServiceClient.from_connection_string(os.getenv('DataStorage'))
        container_client = blob_service_client.get_container_client(os.getenv('DataContainer'))
        blob_client: BlobStorageClient = BlobStorageClient(container_client)
        batches = blob_client.get_batches(os.getenv('DataSubpath'))

        if len(batches) == 0:
            logging.warning('No new batches to validate')
            return

        # Send batches to the validation queue
        for batch in batches:
            batch.status = Status.RUNNING
            blob_client.save_batch_status(batch)
            logging.info('Sending batch %s > %s to the validation queue',
                batch.customer, batch.timestamp)

        myqueue.set(map(lambda b: b.serialize(), batches))
        logging.info('%s new batches sent to the validation queue', len(batches))

    except Exception as ex:
        logging.exception('EXCEPTION while getting batches', exc_info=ex)
