"""
Descripccion Funcion:

El obejtivo de la función esta en permitir cargar de manera segura variables almacenadas en un archivo YAML, 
facilitando su uso posterior en las funciones que se requieran posteriomente.

Parametros:

    -RutaArchivoVariablesProceso: Type STR
"""


import yaml

def read_metadata(RutaArchivoVariablesProceso):
    #Objeto encargado de abrir el archivo de parametros/variables
    with open(RutaArchivoVariablesProceso, 'r') as file:
        #Cargue de las variables procedentes del archivo YAML.
        VariablesProceso = yaml.safe_load(file)

    return VariablesProceso