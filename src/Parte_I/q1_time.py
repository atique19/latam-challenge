

from time import time
from datetime import datetime
from typing import List, Tuple
from packages.data_processor import DataProcessor
from memory_profiler import profile

@profile
def q1_time(data_procesor: DataProcessor) -> List[Tuple[datetime.date, str]]:
    """
    Procesa los datos del archivo especificado utilizando el metodo 
    (procces_data_1_time) de la clase (DataProcessor).

    Args:
        ruta_archivo (str): Ruta al archivo JSON que contiene los datos a procesar.

    Returns:
        List[Tuple[datetime.date, str]]: Una lista de tuplas.

    """

    tiempo_inicial = time()  # Registrar el tiempo inicial antes de procesar los datos
    resultado = data_procesor.procces_data_1_time() # Llamar al metodo para procesar los datos
    data_procesor.determinate_duration(tiempo_inicial) # Medir y mostrar el tiempo de procesamiento
 
    return resultado
