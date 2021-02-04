"""
Validation of a batch of files.
"""

from datetime import datetime, time
import logging
import os
from dateutil import parser
from dateutil.tz import tzutc

from ..common import Batch, Status, BlobStorageClient

INVALID_FOLDER = "invalid"
VALID_FOLDER = "valid"
VALID_ENCODING = "UTF-8-SIG"
COLUMNS = {
    'type1':4,
    'type2':4,
    'type3':14,
    'type4':3,
    'type5':2,
    'type7':23,
    'type8':21,
    'type9':5,
    'type10':3}
COLUMN_SEPARATOR = ','
ENCLOSING = '"'

class BatchValidation:
    """
    Class representing a batch validation.
    """

    def __init__(self, blob_client: BlobStorageClient, data: dict):
        self.__blob_client = blob_client
        default_date = datetime.combine(datetime.now(), time(0, tzinfo=tzutc()))
        customer = data['_Batch__customer']
        timestamp = parser.parse(data['_Batch__timestamp'], default=default_date)
        self.__batch = Batch(customer, timestamp)
        self.__batch.status = data['_Batch__status']
        self.__batch.types = data['_Batch__types']

    def validate(self):
        """
        Validates a batch of files.
        """
        errors = []
        try:
            customer = self.__batch.customer
            timestamp = self.__batch.timestamp
            for item in self.__batch.types.items():
                stream = self.__blob_client.download_blob_content(item[1])

                # Check the encoding
                if stream.encoding != VALID_ENCODING:
                    errors.append(f'INVALID ENCODING in batch {customer} > {timestamp}')
                    continue

                # Check the content of the csv file
                for line in stream:
                    line = line.rstrip(os.linesep)

                    # Check the number of columns
                    columns = line.split(COLUMN_SEPARATOR)
                    if len(columns) != COLUMNS[item[0]]:
                        errors.append(f'INVALID COLUMNS in batch {customer} > {timestamp}')
                        break

                    # Check if each field is enclosed in double quotes
                    for field in columns:
                        if not field.startswith(ENCLOSING) or not field.endswith(ENCLOSING):
                            errors.append(f'INVALID ENCLOSING in batch {customer} > {timestamp}')
                            break

            # Set status
            self.__set_validation_status(Status.VALID if len(errors) == 0 else Status.INVALID)

            # Log errors
            for error in errors:
                logging.error(error)

        except Exception as ex:
            self.__set_validation_status(Status.ERROR)
            logging.exception("EXCEPTION while validating batch %s > %s",
                self.__batch.customer, self.__batch.timestamp, exc_info=ex)

    def __set_validation_status(self, status: Status):
        self.__batch.status = status
        self.__blob_client.save_batch_status(self.__batch)
        if status == Status.ERROR:
            return
        self.__move_batch(VALID_FOLDER if status == Status.VALID else INVALID_FOLDER)

    def __move_batch(self, target_folder: str):
        for file_type in self.__batch.types.values():
            self.__blob_client.move_blob(file_type, target_folder)
