
import os
import yaml
from zipfile import ZipFile

class FileHandler():



    def read_metadata(self, ruta_archivo_parametros: str) -> dict:
        """
        Lee un archivo YAML que contiene parámetros o variables de proceso y devuelve
        su contenido como un diccionario.

        Args
        ----
            ruta_archivo_parametros (str): La ruta al archivo YAML que contiene las variables
                                                del proceso.

        Returns
        -------
            dict:
                Un diccionario que contiene los datos cargados desde el archivo YAML
        """
        #Objeto encargado de abrir el archivo de parametros/variables
        with open(ruta_archivo_parametros, 'r', encoding='utf-8') as file:
            #Cargue de las variables procedentes del archivo YAML.
            variables_proceso = yaml.safe_load(file)

        return variables_proceso


    def check_file_json(self, ruta_archivo: str) -> bool:
        """
        Verifica si un archivo JSON existe en la ruta especificada.

        Args
        ----
            ruta_archivo (str): La ruta al archivo JSON que se desea verificar.

        Returns
        -------
            bool:
                `True` si el archivo JSON existe en la ruta especificada, `False` en caso contrario.
        """
        return os.path.exists(ruta_archivo)

    def unzip_file(self, ruta_zip: str, ruta_destino: str):
   
        """
        Extrae el contenido de un archivo ZIP en el directorio actual.

        Args
        ----
            ruta_zip_str (str): La ruta al archivo ZIP que se desea descomprimir. 

        Returns
        -------
            None:
                Esta función no devuelve ningún valor.
        """
        with ZipFile(ruta_zip, 'r') as zObject:
            zObject.extractall(ruta_destino)