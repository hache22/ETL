from bs4 import BeautifulSoup
import requests
import pandas as pd
import numpy as np
import sqlite3 as db3
from datetime import datetime 

# pagina web origen de los datos
# Formato de los Datos Solicitado por la Empresa 
url = 'https://web.archive.org/web/20230902185326/https://en.wikipedia.org/wiki/List_of_countries_by_GDP_%28nominal%29'
atributos_tablas = ["Country", "GDP_USD_millions"]
nombre_db = 'World_Economies.db'
nombre_tabla = 'Countries_by_GDP'
ruta_csv = './Countries_by_GDP.csv'

# creando conexion base de datos
conexion = db3.connect("World_Economies.db")

def extraer(url, atributos_tablas):
    ''' Esta función extrae el requerido
    información del sitio web y la guarda en un marco de datos. 
    La función devuelve el marco de datos para su posterior procesamiento.'''
    pagina= requests.get(url).text # Extraer la pagina web como texto
    datos = BeautifulSoup(pagina, "html.parser") #Pasar archivo HTML a objeto
    df= pd.DataFrame(columns=atributos_tablas) #Cree un DataFrame de pandas vacío llamado df con columnas de la variable table_attribs
    tablas = datos.find_all('tbody')# Se Extrae todos los atributos 'tbody' del objeto HTML 
    filas = tablas[2].find_all('tr') #Luego se extrae todas las filas de la tabla de índice 2 usando el atributo 'tr'.
    """
    Verifique el contenido de cada fila, que tenga el atributo 'td', para conocer las siguientes condiciones.
    a. La fila no debe estar vacía.
    b. La primera columna debe contener un hipervínculo.
    c. La tercera columna no debe ser '—'.
    """
    for fila in filas: # Ejecuté un bucle for y verifiqué las condiciones usando declaraciones if.
        col = fila.find_all('td') #Utilicé la función 'find_all()' del objeto HTML para recopilar todos los atributos de un tipo específico.
        if len(col)!=0:
            if col[0].find('a') is not None and '—' not in col[2]: #Se Tiene en cuenta que '—' es un carácter especial y no un guión general.
                data_dict = {"Country": col[0].a.contents[0],
                             "GDP_USD_millions": col[2].contents[0]}
                df1 = pd.DataFrame(data_dict, index=[0])
                df = pd.concat([df,df1], ignore_index=True) #se utiliza la funcion concat de pandas para agregar los datos 
    return df

def transformar(df):
    '''Esta función convierte la información del PIB de Moneda
    formato a valor flotante, transforma la información del PIB de
    USD (Millones) a USD (Miles de millones) redondeando a 2 decimales.
    La función devuelve el marco de datos transformado.'''
    GDP_list = df["GDP_USD_millions"].tolist() #Guardé la columna del marco de datos como una lista
    #Iteré sobre el contenido de la lista y utilicé las funciones split() y join() para convertir el texto de la moneda en texto numérico
    GDP_list = [float("".join(x.split(','))) for x in GDP_list] 
    GDP_list = [np.round(x/1000,2) for x in GDP_list] #Utilicé la función numpy.round() para redondear.
    df["GDP_USD_millions"] = GDP_list # Asigné la lista modificada nuevamente al marco de datos
    df=df.rename(columns = {"GDP_USD_millions":"GDP_USD_billions"}) # Utilicé la funcion .rename() de pandas para modificar el nombre de la columna de 'GDP_USD_millones' a 'GDP_USD_billions'
    return df

def cargar_a_csv(df, ruta_csv):
    '''Esta función guarda el marco de datos final como un archivo `CSV`
    en la ruta proporcionada.'''
    df.to_csv(ruta_csv) #Utilicé el objeto de función 'to_csv()' para el marco de datos de pandas.

def cargar_a_db(df, conexion, nombre_tabla):
    ''' Esta función guarda el marco de datos final como una tabla de base de datos.
    con el nombre proporcionado.'''
    df.to_sql(nombre_tabla , conexion ,if_exists="replace" , index=False)#Utilicé el objeto de función 'to_sql()' para el marco de datos de pandas.


def consultar(declaración_consulta, conexion_sql):
    '''Esta función ejecuta la consulta indicada en la tabla de la base de datos y
    imprime la salida en el terminal.'''
    print(declaración_consulta)
    salida_consulta = pd.read_sql(declaración_consulta,  conexion_sql)
    print(salida_consulta )

def log_progress(mensaje):
    ''' Esta función registra el mensaje mencionado en una etapa determinada de la ejecución del código en un archivo de registro. La función no devuelve nada'''
    ''' Aca se definen las entidades requeridas y se llama a las correspondientes.
    funciones en el orden correcto para completar el proyecto. .'''
    timestamp_format = '%Y-%h-%d-%H:%M:%S' # Año-Mes-Dia-Hora-Minutos-Segundos 
    now = datetime.now() # fecha-hora actual
    timestamp = now.strftime(timestamp_format) 
    with open("./etl_project_log.txt","a") as f: 
        f.write(timestamp + ' : ' + mensaje + '\n')


log_progress('Fase Preliminar Completa. Iiniciando el proceso de Extraccion Transformacion y Carga')
df = extraer(url, atributos_tablas)
log_progress('Extraccion de Datos Completa. Iniciando el Proceso de Transformacion')
df = transformar(df)
log_progress('Transformacion de los Datos Completa. Iniciando el Proceso de Carga')
cargar_a_csv(df, ruta_csv)
log_progress('Datos Guardados en archivo CSV')
conexion_sql = db3.connect('World_Economies.db')
log_progress('Conexion a la Base de Datos SQL comenzando .')
cargar_a_db(df, conexion_sql, nombre_tabla)
log_progress('Datos cargados en la base de datos como tabla. Ejecutando la consulta')
declaración_consulta = f"SELECT * from {nombre_tabla} WHERE GDP_USD_billions >= 100"
consultar(declaración_consulta, conexion_sql)
log_progress('Proceso ETL Completo!.')
conexion.close()