"""
Clase encargada de contener metodos para la ejecucion de cada una de los requerimientos.
"""
import json ,emoji 
from datetime import datetime
from typing import List, Tuple
from collections import Counter
from pyspark.sql import SparkSession

class DataProcessor:
    
    

    @staticmethod
    def read_data_part1( RutaArchivo: str, Query: str, NombreTablaTmp: str) -> List[Tuple[datetime.date, str]]:

        spark = SparkSession.builder \
            .appName("Data Processor") \
            .getOrCreate()
        try:
            # Leer archivo JSON y convertirlo en DataFrame
            df = spark.read.json(RutaArchivo)

            # Crear una tabla temporal en base al DataFrame obtenido
            df.createOrReplaceTempView(NombreTablaTmp)
            
            # Ejecutar la consulta
            resultado = spark.sql(Query)
            
            #Pasar el resultado del dataframe a rows con la funcion .collect()
            filas = resultado.collect()

            #Mapear cada row a una tupla
            lista_tuplas = list(map(lambda row: tuple(row), filas))


        except Exception as e:
            raise ValueError(f"LOGREPORTE: Revisar los parámetros de entrada de la función o revisar el archivo JSON: {e}")

        return lista_tuplas


    def read_data_part2( RutaArchivo: str, LlavePrimaria: str) -> List[Tuple[str, int]]:

        def extract_emojis(data: str):
            return ''.join(c for c in data if c in emoji.EMOJI_DATA)

        ListaTweets = []
        #Objeto encargado de abrir el archivo json
        with open(RutaArchivo, "r" ) as f:
            #Realizar iteracion sobre cada linea del archivo
            for linea in f:
                #Cargar cada linea como json
                LineasData = json.loads(linea)
                #Leer llave requeria
                content = LineasData[LlavePrimaria]
                #Extraer emojis encontrados en la llave
                emojis_in_row = extract_emojis(content)
                #Condicion para no tomar las lineas que no contengan emojis
                if emojis_in_row != "":
                    #Realizar agregacion de valores en la lista
                    ListaTweets.append(emojis_in_row)

        #Agurpar valores en una sola lista
        UnirItemsLista = ''.join(ListaTweets)
        #Conteo de valores almacenados en la lista con la funcio Counter
        ConteoEmojis = Counter(UnirItemsLista)

        #Realizar una lista comprimida con los emojis y lo valores totales econtrados.
        FrecuenciaEmoji = [(emoji, count) for emoji, count in ConteoEmojis.items()]

        #Filtrado de de valores de la tupla para organizarlos de manera descendente
        FrecuenciaEmoji.sort(key=lambda x: x[1], reverse=True)
        
        #Extraer los 10 primeros valores de la lista filtrada
        EmojisTop10 = FrecuenciaEmoji[:10]

        return EmojisTop10



    def read_data_part3(RutaArchivo: str, LlavePrimaria:str, LlaveSecundaria: str) -> List[Tuple[str, int]]:

        ListaUsuariosMencionados = []

        #Objeto encargado de abrir el archivo json
        with open(RutaArchivo, "r" ) as f:
            #Realizar iteracion sobre cada linea del archivo
            for linea in f:
                #Cargar cada linea como json
                LineasData = json.loads(linea)
                #Condicion para extraer registros que contengan solo la llave primaria con valores.
                if LineasData[LlavePrimaria] != None  :
                    #Extraer contenido almaecenado en la llave primaria
                    ContenidoLlave = LineasData[LlavePrimaria]
                    #Realizar lista comprimida que contenga la cantidad de usuarios mencionados en un solo tweet
                    usernames = [Usuarios[LlaveSecundaria] for Usuarios in ContenidoLlave]
                    #Agregaciion de valores en la lista de usuarios mencioandos
                    ListaUsuariosMencionados.append(usernames)

        #Agrupacion de todos los valores obtenidos en una sola lista
        ListaUsuarios = [ValorLista for ListaUnitariaUsuarios in ListaUsuariosMencionados for ValorLista in ListaUnitariaUsuarios]

        #Conteo de valores unicos almacenados en la lista con la funcio Counter
        ConteoUsuariosLista = Counter(ListaUsuarios)

        #Realizar una lista comprimida con los el nombre de usuario y numero de menciones.
        ListaUsuarioConteoMenciones = [(Usuario, ConteoMenciones) for Usuario, ConteoMenciones in ConteoUsuariosLista.items()]

        #Filtrado de de valores de la tupla para organizarlos de manera descendente
        ListaUsuarioConteoMenciones.sort(key=lambda x: x[1], reverse=True)

        #Extraer los 10 primeros valores de la lista filtrada
        MencionesTop10 = ListaUsuarioConteoMenciones[:10]

        return MencionesTop10
