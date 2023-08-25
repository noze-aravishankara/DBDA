from datetime import datetime, tzinfo, timezone
from pymongo import MongoClient
import logging
import atexit
import csv

logging.basicConfig(level=logging.INFO)


class DA_Mongo_Access:
    def __init__(self, sensorId=None, start_dt=None, end_dt=None, file_save_location=None):
        self.sensorId = sensorId
        self.start_dt = start_dt
        self.end_dt = end_dt
        self.file_save_location = file_save_location

        self.start_mongo_client()
        if self.sensorId and start_dt:
            self.filter_for_data()

    def start_mongo_client(self):
        self.client = MongoClient('mongodb://pd1-olaf:QYVjJcuwTbAkM3hCEqdWj49w@10.16.0.10:30027,10.16.0.11:30028,10.16.0.9:30029/persistence-service?tls=false&replicaSet=rs0&authMechanism=DEFAULT&authSource=persistence-service')
        logging.info("Connected to Client")

    def filter_for_data(self):
        _ = { # FILTER PARAMETERS
            'sn': self.sensorId,
            'createdAt': {
                '$gte': self.start_dt,
                '$lte': self.end_dt
                }
            }
        
        self.filtered_data = self.client['persistence-service']['TelemetryLog'].find(filter=_)
        self.write_data_to_csv()

    def write_data_to_csv(self):
        _ = 0
        with open(self.file_save_location, "w", newline="") as csvfile:
            csv_writer = csv.DictWriter(csvfile, fieldnames=self.filtered_data[0].keys())
            csv_writer.writeheader()
            for document in self.filtered_data:
                csv_writer.writerow(document)
                _ += 1

        logging.info(f"Saved Data to {self.file_save_location}. There are {_} documents saved.")
        

    @atexit.register
    def exit_handler(self):
        self.client.close()
        logging.info("Closed the client")


if __name__ == "__main__":
    DA_Mongo_Access(sensorId='0000000616-1623-brz-nz', 
                    start_dt=datetime(2023, 8, 25, 13, 0, 0, tzinfo=timezone.utc), 
                    end_dt=datetime(2023, 8, 25, 14, 0, 0, tzinfo=timezone.utc),
                    file_save_location='data/test.csv')