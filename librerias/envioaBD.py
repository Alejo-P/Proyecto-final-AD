#Importación de librerías
import pandas as pd
from sqlalchemy import create_engine

# Cargar los archivos .CSV mediante la librería pandas
datos = pd.read_csv("eleccionesGenerales2023.csv")
registro = pd.read_csv("ciencia_naturalist_ene2024.csv")
basquetbol = pd.read_csv("campeonato_mundial_basketball.csv", encoding="latin1")
# Corregir los nombres de las columnas
registro.columns = registro.columns.str.strip()
#Para generar la conexión con la Base de Datos
engine = create_engine("mysql+mysqlconnector://ussbzqxxrbaktocd:4jUZNEC8FPFOJasnBN7L@b7bg9l2v5kzw6mr8copl-mysql.services.clever-cloud.com/b7bg9l2v5kzw6mr8copl")

try:
    # Insertar datos 
    #el if_exists se utiliza para saber si existe la tabla , en caso de existir, se eliminará y 
    #se creará una nueva tabla con los mismos nombre y esquema por el valor replace
    datos.to_sql('ELECCIONES_GENERALES', con=engine, if_exists='replace', index=False)
    registro.to_sql('REGISTRO_CIENCIA', con=engine, if_exists='replace', index=False)
    #El parámetro 'con' se utiliza para especificar la conexión a la base de datos 
    basquetbol.to_sql('MUNDIAL_BASKETBALL', con=engine, if_exists='replace', index=False)
    print("Datos insertados correctamente.")

except Exception as e:
    print("Error al insertar los datos:", e)


