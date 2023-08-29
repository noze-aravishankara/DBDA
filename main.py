from src.data_request import DA_Mongo_Access
from datetime import datetime, tzinfo, timezone


if __name__ == "__main__":
    DA_Mongo_Access(sensorId='0000000616-1623-brz-nz', 
                    start_dt=datetime(2023, 8, 25, 13, 0, 0, tzinfo=timezone.utc), 
                    end_dt=datetime(2023, 8, 25, 14, 0, 0, tzinfo=timezone.utc),
                    file_save_location='data/test.csv')
    
