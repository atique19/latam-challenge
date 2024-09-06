"""
Funcion basada en la libreria cprofile. Cuyo objetivo es evidenciar el timepo en ejecucion 
de todas la llamadas de la funcion que se requiere ejecutar y medir.

PARAMETROS IN:
    file_path : Ruta archivo JSON - type STR
    primary_key : Llave primaria del Json - type STR
    second_key : Llave secundaria del Json - type STR
"""

import cProfile
from typing import List, Tuple
from datetime import datetime
from packages.DataProcessor import DataProcessor


def q3_time(file_path: str, primary_key: str, second_key: str) -> List[Tuple[datetime.date, str]]:
    
    cProfile.runctx('DataProcessor.read_data_part3(file_path, primary_key, second_key)', globals(), locals(),filename=None, sort="cumtime")
