from typing import List, Tuple
from datetime import datetime
from memory_profiler import profile, memory_usage
from DataProcessor import DataProcessor

"""
Funcion basada en la libreria memory_profiler==0.61.0. Cuyo objetico es evidenciar el consumo en recursos  
(Memoria) para la funcion principal destinada al procesamiento de los datos.

PARAMETROS IN:
file_path  : Ruta archivo JSON - type STR
query      : Query del proceso - type STR
table_name : Nombre tabla tmp  - type STR
"""

@profile
def q1_memory(file_path: str, query: str, table_name: str) -> List[Tuple[datetime.date, str]]:
    
    #Variable definida usando la funcion memory_usage donde se parametrizan la funcion y sus argumentos se extrae el tamaño maximo que requirio en memoria el proceso (MiB)
    mem_usage = memory_usage((DataProcessor.read_data_part1, (file_path, query, table_name), { }), max_usage=True)
    print(f"LOGREPORTE: EL CONSUMO EN MEMORIA DEL PROCESO ES DE: {mem_usage} MiB")