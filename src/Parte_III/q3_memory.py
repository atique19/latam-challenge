from time import time
from typing import List, Tuple
from datetime import datetime
from packages.DataProcessor import DataProcessor
from memory_profiler import profile, memory_usage



@profile
def q3_memory(data_procesor: DataProcessor) -> List[Tuple[datetime.date, str]]:
    """
    Procesa los datos del archivo especificado utilizando el metodo 
    (read_data_part3_memory) de la clase (DataProcessor).

    Args:
        data_procesor (DataProcessor): Clase principal qque contiene toda la logica.

    Returns:
        List[Tuple[datetime.date, str]]: Una lista de tuplas.

    """

    tiempo_inicial = time()  # Registrar el tiempo inicial antes de procesar los datos
    resultado = data_procesor.read_data_part3_memory() # Llamar al metodo para procesar los datos
    data_procesor.determinate_duration(tiempo_inicial) # Medir y mostrar el tiempo de procesamiento
 
    return resultado