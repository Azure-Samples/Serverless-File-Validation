"""
Batch of files in Azure Blob Storage.
"""
from datetime import datetime
from enum import Enum, auto
import json
import jsonpickle

# Types of files in a batch
TYPES = ['type1', 'type2', 'type3', 'type4', 'type5', 'type7', 'type8', 'type9', 'type10']

# Index of TYPES array for the reference type (used to set the status of the batch in its blob
# metadata)
REF_TYPE = 0

class Status(Enum):
    """
    Status of the validatin of a batch.
    This status will be set in the metadata of the blob referenced by TYPES[REF_TYPE]
    """
    ERROR = auto()
    RUNNING = auto()
    VALID = auto()
    INVALID = auto()

class Batch:
    """
    Class representing a batch of files in Azure Blob Storage.
    A batch of files is comprised of several .csv files, one per type in TYPES array.    
    Path for csv files:
    {subpath}/{customer}_{date}_{time}_{type}.csv
    Example:
    input_data/cust1_20171010_1112_type1.csv
    """

    def __init__(self, customer: str, timestamp: datetime):
        self.__customer = customer
        self.__timestamp = timestamp
        self.__types = {}
        self.__status = None

    @property
    def customer(self):
        """
        Customer name.
        """
        return self.__customer

    @property
    def timestamp(self):
        """
        Datetime when the batch was created.
        """
        return self.__timestamp

    @property
    def types(self):
        """
        Dictionary of .csv files (should have one per type in TYPES array).
        """
        return self.__types

    @types.setter
    def types(self, types: dict):
        self.__types = types

    @property
    def status(self):
        """
        Status of the batch validation.
        """
        return self.__status

    @status.setter
    def status(self, status: Status):
        self.__status = status

    def is_complete(self):
        """
        True if all types in TYPES array are present in the batch.
        """
        for file_type in TYPES:
            if not self.__types.get(file_type):
                return False
        return True

    def serialize(self) -> str:
        """
        Serializes the contents of this class as json.
        """
        encoded_batch = jsonpickle.encode(self, unpicklable=False)
        return json.dumps(encoded_batch, indent=4)
