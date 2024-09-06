

import json
import emoji
import pandas as pd
from time import time
from datetime import datetime
from typing import List, Tuple
from collections import Counter
#from pyspark.sql import SparkSession
from collections import defaultdict
#from pyspark.sql.window import Window
#from pyspark.sql.functions import col, explode, date_format, count, to_date, rank ,max as spark_max


class DataProcessor:
   
    """
    Clase Contenedora de las funciones principales de cada uno de los  requerimientos del challenge.
    """

    def __init__(self, ruta_archivo):
        #self.session_spark = self.create_spark_session()
        self.ruta_archivo = ruta_archivo
        self.df_pd = pd.read_json(ruta_archivo, lines=True)

    #@staticmethod
    def determinate_duration(self, tiempo_inicial: time):
        """
        Calcula la duración transcurrida desde un tiempo 
        inicial hasta el momento actual y la imprime.

        Args
            initial_time (float): El tiempo inicial en segundos 
            desde el epoch (1970-01-01 00:00:00 UTC).

        Returns
        -------
            None: Este método imprime la duración transcurrida en lugar de devolver un valor.
        """
        return print(f"Duracion : {time() - tiempo_inicial} --")

    def procces_data_1_time(self):

        """
        Procesa un archivo JSON para identificar los usuarios con más tweets 
        en las fechas con más actividad.

        Args:
            file_path (str): Ruta al archivo JSON que contiene los datos a procesar.

        Returns:
            List[Tuple[datetime.date, str]]: Una lista de tuplas donde cada tupla contiene.
        """
        # Cargar el archivo JSON en un DataFrame
        df = self.df_pd

        # Convertir 'date' a tipo datetime.date y extraer 'username'
        df['date'] = pd.to_datetime(df['date']).dt.date
        df['username'] = df['user'].apply(lambda x: x['username'])

        # Contar el número de tweets por fecha
        tweets_por_fecha = df.groupby('date').size()

        # Obtener las 10 fechas con más tweets
        top_fechas = tweets_por_fecha.nlargest(10).index

        # Filtrar el DataFrame para las top 10 fechas
        top_dates_df = df[df['date'].isin(top_fechas)]

        # Contar el número de tweets por usuario por fecha
        tweets_por_usr_fecha = top_dates_df.groupby(['date', 'username'])\
                                                .size().reset_index(name='count')

        # Encontrar el usuario con más tweets por cada fecha
        top_usr_por_fecha = tweets_por_usr_fecha.loc[tweets_por_usr_fecha
                                                          .groupby('date')['count'].idxmax()]

        # Convertir el DataFrame resultante a una lista de tuplas
        resultado = [(date, username) for date, username in zip(top_usr_por_fecha['date'],
                                                                 top_usr_por_fecha['username'])]

        return resultado


    def process_data_1_memory(self):

        """
        Procesa datos desde un archivo JSON línea por línea para contar tweets por usuario en 
        cada fecha, y devuelve las fechas con el usuario con más tweets para cada una de las 
        10 fechas con mayor actividad.

        Args
        ----
            file_path (str): La ruta al archivo JSON que contiene los datos de tweets. 
                            Cada línea del archivo ebe ser un objeto JSON con campos 'date' y 'user'

        Returns
        -------
            List[Tuple[datetime.date, str]]
                Una lista de tuplas, donde cada tupla contiene una fecha (como `datetime.date`)

        """
        # Crear un defaultdict para contar tweets por fecha y usuario
        dates_dict = defaultdict(lambda: defaultdict(int))
        with open(self.ruta_archivo, 'r', encoding="utf-8") as f:

            # Analizar el archivo línea por línea para optimizar el uso de memoria
            for line in f:
                tweet = json.loads(line)
                tweet_date = tweet['date'].split('T')[0]
                username = tweet['user']['username']

                # Actualizar el contador de tweets por usuario en cada fecha
                dates_dict[tweet_date][username] += 1

        # Ordenar las fechas segen el numero total de tweets en cada fecha y seleccionar
        # las 10 principales
        top_dates = sorted(dates_dict.keys(), key=lambda x: sum(dates_dict[x].values()),
                           reverse=True)[:10]

        # Obtener el usuario con más tweets para cada una de las fechas principales
        top_users = [max(dates_dict[date], key=dates_dict[date].get) for date in top_dates]

        # Convertir las fechas a formato datetime.date()
        top_dates = [datetime.strptime(date_str, "%Y-%m-%d").date() for date_str in top_dates]

        return list(zip(top_dates, top_users))

    def read_data_part2_memory(self) -> List[Tuple[str, int]]:

        """
        Lee un archivo JSON línea por línea, extrae y cuenta los emojis en el contenido asociado 
        con una llave primaria, y devuelve los 10 emojis más frecuentes junto con su cuenta.

        Args
        ----
            None: Esta función utiliza el DataFrame de pandas de la instancia de la clase 
                donde está definida.

        Returns
        -------
            List[Tuple[str, int]]
                Una lista de tuplas, donde cada tupla contiene un emoji y su frecuencia de 
                aparición en el contenido asociado con la llave primaria.

        """
        def extract_emojis(data: str):
            """
            Extrae y devuelve todos los emojis encontrados en una cadena de texto.

            Args
            ----
                data (str): La cadena de texto de la cual se extraerán los emojis.

            Returns
            -------
                str
                    Una cadena de texto que contiene todos los emojis encontrados 
                    en el texto de entrada.
            """
            return ''.join(c for c in data if c in emoji.EMOJI_DATA)

        lista_tweets = []
        #Objeto encargado de abrir el archivo json
        with open(self.ruta_archivo, "r", encoding="utf-8") as f:
            #Realizar iteracion sobre cada linea del archivo
            for linea in f:
                #Cargar cada linea como json
                lineas_data = json.loads(linea)
                #Leer llave requeria
                content = lineas_data['content']
                #Extraer emojis encontrados en la llave
                emojis_in_row = extract_emojis(content)
                #Condicion para no tomar las lineas que no contengan emojis
                if emojis_in_row != "":
                    #Realizar agregacion de valores en la lista
                    lista_tweets.append(emojis_in_row)

        #Agurpar valores en una sola lista
        unir_item_lista = ''.join(lista_tweets)
        #Conteo de valores almacenados en la lista con la funcio Counter
        conte_emojis = Counter(unir_item_lista)

        #Realizar una lista comprimida con los emojis y lo valores totales econtrados.
        frecuencia_emoji = [(emoji, count) for emoji, count in conte_emojis.items()]

        #Filtrado de de valores de la tupla para organizarlos de manera descendente
        frecuencia_emoji.sort(key=lambda x: x[1], reverse=True)

        # Extraer los 10 emojis más frecuentes
        emojis_top = frecuencia_emoji[:10]

        return emojis_top

    def read_data_part2_time(self) -> List[Tuple[str, int]]:
        """
        Extrae y cuenta los emojis en el contenido asociado con el DataFrame de pandas 
        y devuelve los 10 emojis más frecuentes junto con su cuenta.

        Args
        ----
            None: Esta función utiliza el DataFrame de pandas de la instancia de la clase 
                    donde está definida.

        Returns
        -------
            List[Tuple[str, int]]:
                Una lista de tuplas, donde cada tupla contiene un emoji y su frecuencia.
        """
        def extract_emojis(data: str) -> str:
            """
            Extrae y devuelve todos los emojis encontrados en una cadena de texto.

            Args
            ----
                data (str): Cadena de texto 

            Returns
            -------
                str:
                    Una cadena de texto que contiene todos los emojis encontrados en el texto 
                    de entrada.
            """
            return ''.join(c for c in data if c in emoji.EMOJI_DATA)

        # Leer el archivo JSON en un DataFrame de pandas
        df = self.df_pd

        # Extraer el contenido asociado con la llave primaria
        df['emojis'] = df['content'].apply(extract_emojis)


        # Concatenar todos los emojis en una sola cadena
        all_emojis = ''.join(df['emojis'])

        # Contar los emojis
        emoji_counts = Counter(all_emojis)

        # Convertir a una lista de tuplas y ordenar
        frecuencia_emoji = sorted(emoji_counts.items(), key=lambda x: x[1], reverse=True)

        # Extraer los 10 emojis más frecuentes
        emojis_top = frecuencia_emoji[:10]
 
        return emojis_top



    def read_data_part3_memory(self) -> List[Tuple[str, int]]:
        """
        Lee un archivo JSON línea por línea, extrae y cuenta los usuarios mencionados 
        en el contenido asociado con una llave primaria y devuelve los 10 usuarios 
        más mencionados junto con el número de menciones.

        Args
        ----
            None: Esta función utiliza la ruta del archivo JSON especificado.

        Returns
        -------
            List[Tuple[str, int]]:
                Una lista de tuplas, donde cada tupla contiene un nombre de usuario y el número 
                de veces que fue mencionado en el archivo JSON. 
        """
        lista_usr_mecionados = []

        #Objeto encargado de abrir el archivo json
        with open(self.ruta_archivo, "r", encoding="utf-8") as f:

            #Realizar iteracion sobre cada linea del archivo
            for linea in f:
                #Cargar cada linea como json
                lineas_data = json.loads(linea)
                #Condicion para extraer registros que contengan solo la llave primaria con valores.
                if lineas_data['mentionedUsers'] is not None:
                    #Extraer contenido almaecenado en la llave primaria
                    contenido_llave = lineas_data['mentionedUsers']
                    #Realizar lista comprimida que contenga la cantidad de usuarios 
                    # mencionados en un solo tweet
                    usernames = [Usuarios['username'] for Usuarios in contenido_llave]
                    #Agregaciion de valores en la lista de usuarios mencioandos
                    lista_usr_mecionados.append(usernames)

        #Agrupacion de todos los valores obtenidos en una sola lista
        lista_usuarios = [ValorLista for lista_usuarios_unitaria in
                         lista_usr_mecionados for ValorLista in lista_usuarios_unitaria]

        #Conteo de valores unicos almacenados en la lista con la funcio Counter
        conteo_usuarios = Counter(lista_usuarios)

        #Realizar una lista comprimida con los el nombre de usuario y numero de menciones.
        lista_usuarios_conteo = [(Usuario, ConteoMenciones) for 
                                 Usuario, ConteoMenciones in conteo_usuarios.items()]

        #Filtrado de de valores de la tupla para organizarlos de manera descendente
        lista_usuarios_conteo.sort(key=lambda x: x[1], reverse=True)

        #Extraer los 10 primeros valores de la lista filtrada
        menciones_top = lista_usuarios_conteo[:10]

        return menciones_top



    def read_data_part3_time(self) -> List[Tuple[str, int]]:

        """
        Extrae y cuenta los usuarios mencionados en el contenido del DataFrame de pandas 
        y devuelve los 10 usuarios mas mencionados junto con el número de menciones.

        Args
        ----
            None: Esta función utiliza el DataFrame de pandas de la instancia de la clase 
                donde está definida. No requiere argumentos externos.

        Returns
        -------
            List[Tuple[str, int]]:
                Una lista de tuplas, donde cada tupla contiene un nombre de usuario y el número 
                de veces que fue mencionado en el DataFrame de pandas.
        """
        def extract_usernames(data: list, key: str) -> List[str]:
            """
            Extrae los nombres de usuario de una lista de diccionarios.
            """
            return [user[key] for user in data] if data else []

        # Leer el archivo JSON en un DataFrame de pandas
        df = self.df_pd
        
        # Filtrar las filas que contienen datos en la llave primaria
        df = df[df['mentionedUsers'].notna()].copy()

        # Extraer los nombres de usuario usando la función definida
        df['usernames'] = df['mentionedUsers'].apply(lambda x: extract_usernames(x, 'username'))

        # Unir todos los nombres de usuario en una sola lista
        all_usernames = [username for sublist in df['usernames'] for username in sublist]

        # Contar los nombres de usuario
        username_counts = Counter(all_usernames)

        # Convertir a una lista de tuplas y ordenar
        frecuencia_usuario = sorted(username_counts.items(), key=lambda x: x[1], reverse=True)

        # Extraer los 10 usuarios más mencionados
        usuarios_top = frecuencia_usuario[:10]

        return usuarios_top

