"""
Funcion basada en la libreria cprofile. Cuyo objetivo es evidenciar el timepo en ejecucion 
de todas la llamadas de la funcion que se requiere ejecutar y medir.

PARAMETROS IN:
file_path : Ruta archivo JSON - type STR
primary_key : Nombre de la llave requeria donde estan almacenados los tweets - type STR
"""

from typing import List, Tuple
from datetime import datetime
import cProfile
from DataProcessor import DataProcessor


def q2_time(file_path: str, primary_key: str) -> List[Tuple[datetime.date, str]]:
    
    cProfile.runctx('DataProcessor.read_data_part2(file_path, primary_key)', globals(), locals(),filename=None, sort="cumtime")
