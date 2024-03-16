from typing import List, Tuple
from datetime import datetime
import cProfile
from DataProcessor import DataProcessor


def q1_time(file_path: str, query: str, table_name: str) -> List[Tuple[datetime.date, str]]:
    
    cProfile.runctx('DataProcessor.read_data_part1(file_path, query,table_name)', globals(), locals(),filename=None, sort="cumtime")

   

