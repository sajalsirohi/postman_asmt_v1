#!/usr/bin/env python3

import os
from multiprocessing.pool import ThreadPool

from azure.storage.blob import BlobServiceClient, ContainerClient
from dotenv import load_dotenv

from main import postman_main
from sql_utilities import SQL, ROOT_DIR

load_dotenv()

CONNECTION_STRING = os.getenv('blob_conn_string')


class BlobGetData:
    """
    A blob interface class.
    Refrenced from
     https://github.com/sajalsirohi/everythingdata/blob/azure/azure_utilities/azure_file_storage/blob/blob_get_data.py
    """

    def __init__(self,
                 connection_string=CONNECTION_STRING,
                 **options):
        self.container_name       = options.get('container_name', "defaultcontainerpython")
        self.connection_string    = connection_string
        self.blob_service_client  = BlobServiceClient.from_connection_string(connection_string)
        self.container_client     = ContainerClient.from_connection_string(connection_string, self.container_name)
        self.local_dir_path       = os.path.join(ROOT_DIR, "data")

    def list_all_blobs(self):
        """
        List all the blobs in the container
        :return:
        """
        blob_list = self.container_client.list_blobs()
        return [b for b in blob_list]

    def run(self, blobs):
        # Download 10 files at a time!
        with ThreadPool(processes=int(10)) as pool:
            return pool.map(self._save_blob_locally, blobs)

    def _save_blob_locally(self, blob):
        try:
            file_name = blob.name
        except AttributeError:
            file_name = blob
        print(f"Downloading file --> {file_name}")
        bytes_ = self.container_client.get_blob_client(file_name).download_blob().readall()

        download_file_path = os.path.join(self.local_dir_path, file_name)
        os.makedirs(os.path.dirname(download_file_path), exist_ok=True)

        with open(download_file_path, "wb") as file:
            file.write(bytes_)
        return file_name

    def download_these_blobs(self, blob_list):
        """
        Downlaad the blobs specified in the list
        :param blob_list:
        :return:
        """
        for blob in blob_list:
            self.download_this_blob(blob_name=blob)

    def download_all_blobs(self, local_dir_path = None):
        """
        Download all the blobs in the defined loca_dir_path
        :param local_dir_path:
        :return:
        """
        self.local_dir_path = local_dir_path if local_dir_path else self.local_dir_path
        my_blobs = self.list_all_blobs()
        result = self.run(my_blobs)
        print(result)

    def download_this_blob(self, blob_name, local_dir_path = None, **options):
        """
        Interact with a particular blob
        :param local_dir_path: path where you want to download this blob
        :param blob_name: Name of the blob to download
        :return:
        """
        self.local_dir_path = local_dir_path if local_dir_path else self.local_dir_path
        self._save_blob_locally(blob_name)


def main():
    """
    Main driver
    :return:
    """
    blob_class = BlobGetData()
    all_blobs = blob_class.list_all_blobs()

    sql = SQL(
        host_name     = "localhost",
        database_name = "master",
        username      = "sa",
        password      = "Igobacca1@",
    )

    sql.check_connection(skip_table_creation=True)
    last_processed = sql.latest_processed_time()
    blobs_to_process = []
    for blobs in all_blobs:
        if blobs.last_modified > last_processed:
            blobs_to_process.append(blobs)
    if blobs_to_process:
        blob_class.download_these_blobs(blobs_to_process)
        postman_main()


if __name__ == '__main__':
    main()