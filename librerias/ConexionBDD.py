import mysql.connector, random, string
import pandas as pd
from pymongo import MongoClient, errors
# Conexion a una base de datos MySQL
class ConexionMySQL:
    def __init__(self):
        self._consultas=[]
        self._ID_query=str()
    
    def Conexion(self, host, user, password, db):
        """_Conexion a la base de datos MySQL_

        Args:
            host (_str_): _Servidor de la base de datos MySQL_
            user (_str_): _Usuario para la conexion al servidor_
            password (_str_): _Contraseña de acceso al servidor_
            db (_str_): _Base de datos que se va a utilizar_

        Returns:
            bool: True si la conexión es exitosa, False en caso contrario
        """
        try:
            self._conexion=mysql.connector.connect(
                host=host,
                user=user,
                password=password,
                database=db
            )
            self._cursor=self._conexion.cursor()
            self._resultado=True
        except mysql.connector.Error as e:
            self._resultado=False
        return self._resultado
    
    def Consulta(self, SQL_query, params=None):
        """
        Consulta SQL

        Args:
            SQL_query (str): Consulta que se desea realizar a la base de datos
            params (tuple): Parámetros para la consulta SQL (opcional)

        Raises:
            ValueError: Si la sentencia ingresada no pertenece a una sentencia de consulta "SELECT"

        Returns:
            str: ID de la consulta
        """
        self._find=False
        if str(SQL_query).startswith(("SELECT", "SHOW", "DESCRIBE")):
            if params:
                self._cursor.execute(SQL_query, params)
            else:
                self._cursor.execute(SQL_query)
            self._info=self._cursor.fetchall()
            for consulta in self._consultas:
                if self._info == consulta["Consulta"]:
                    self._ID_query = str(consulta["ID"])
                    self._find=True
                    break
            if not self._find:
                self._ID_query="".join(random.choices(string.ascii_letters+string.digits,k=5))
                self._consultas.append({"ID":self._ID_query, "Consulta": self._info})
            return self._ID_query
                
        else:
            raise ValueError(f"La sentencia '{SQL_query}' no es una sentencia de consulta valida")
    
    def Modificar(self, SQL_query, params=None):
        """
        Modificacion de registros en la base de datos

        Args:
            SQL_query (str): Sentencia de modificacion de registros
            params (tuple o list): Parámetros para la consulta SQL (opcional). Puede ser una única tupla o una lista de tuplas.

        Raises:
            ValueError: Si la sentencia ingresada no pertenece a una sentencia DML o DDL válida
            ExcepcionIntegridad: Si el registro a insertar ya existe en la base de datos

        Returns:
            int: Numero de filas afectadas por la consulta
        """
        self._find=False
        self._sentenciasSQL=["INSERT", "UPDATE", "DELETE", "ALTER", "DROP", "CREATE"]
        try:
            for sentencia in self._sentenciasSQL:
                if str(SQL_query).startswith(sentencia):
                    if params:
                        if isinstance(params, tuple):  # Si params es una tupla
                            self._ejecuion = self._cursor.execute(SQL_query, params)
                        elif isinstance(params, list):  # Si params es una lista de tuplas
                            self._ejecuion = self._cursor.executemany(SQL_query, params)
                        else:
                            raise ValueError("Los parámetros deben ser una única tupla o una lista de tuplas")
                    else:
                        self._ejecuion = self._cursor.execute(SQL_query)
                    self._conexion.commit()
                    return self._ejecuion
            if not self._find:
                raise ValueError(f"La sentencia '{SQL_query}' no es una sentencia de consulta valida")
        except mysql.connector.IntegrityError:
            raise ExcepcionIntegridad("Clave duplicada en el registro de la base de datos")
        except mysql.connector.Error as e:
            print("Error:", e)

    
    # Exclusivos para los DataFrames
    #---------------------------------------------------------------------------------------#
    def Crear_tabla(self, nombre_tabla, dataframe):
        """Crear una nueva tabla en la base de datos MySQL basada en un DataFrame de Pandas

        Args:
            nombre_tabla (str): Nombre de la tabla que se creará
            dataframe (pd.DataFrame): DataFrame de Pandas que define la estructura de la tabla

        Raises:
            ValueError: Si el DataFrame está vacío o no contiene columnas
        """
        if len(dataframe) == 0 or len(dataframe.columns) == 0:
            raise ValueError("El DataFrame está vacío o no contiene columnas")

        create_table_query = f"CREATE TABLE IF NOT EXISTS {nombre_tabla} ("
        for column_name, dtype in zip(dataframe.columns, dataframe.dtypes):
            if "int" in str(dtype):
                create_table_query += f"{column_name} INT,"
            elif "float" in str(dtype):
                create_table_query += f"{column_name} FLOAT,"
            else:
                create_table_query += f"{column_name} VARCHAR(255),"
        create_table_query = create_table_query[:-1] + ");"

        self.Modificar(create_table_query)
    
    def Insertar_desde_dataframe(self, nombre_tabla, dataframe:pd.DataFrame):
        """Insertar datos desde un DataFrame de Pandas en una tabla de la base de datos MySQL

        Args:
            nombre_tabla (str): Nombre de la tabla en la que se insertarán los datos
            dataframe (pd.DataFrame): DataFrame de Pandas que contiene los datos a insertar
        """
        if len(dataframe) == 0:
            print("El DataFrame está vacío, no hay datos para insertar.")
            return

        columns = ", ".join(dataframe.columns)
        placeholders = ", ".join(["%s"] * len(dataframe.columns))
        insert_query = f"INSERT INTO {nombre_tabla} ({columns}) VALUES ({placeholders});"

        values = [tuple(row) for row in dataframe.values]
        self.Modificar(insert_query, values)
    
    #---------------------------------------------------------------------------------------#
    
    def Obtener_valores(self, ID_Consulta):
        """_Obtener datos de una consulta previa_

        Args:
            ID_Consulta (_str_): _ID de la consulta proporcionada por el metodo Consulta()_

        Returns:
            _Any_: _Valores de la consulta realizada_
        """
        self._find=False
        for consulta in self._consultas:
            if consulta["ID"] == ID_Consulta:
                self._find=True
                self._valor=consulta["Consulta"]
                break
        
        if self._find:
           return self._valor
        else:
            print("No existe una consulta con el ID {}".format(ID_Consulta))
    
    def Cerrar_conexion(self):
        """_Cerrar la conexion a la base de datos_
        """
        try:
            self._consultas.clear()
            self._cursor.close()
            self._conexion.close()
        except AttributeError as e:
            if "_cursor" in str(e) or "_conexion" in str(e):
                pass
        except mysql.connector.Error as em:
            print("Error:", em)

# Conexion a una base de datos MongoDB
class ConexionMongoDB:
    def __init__(self):
        self.client = None
        self.db = None
        self.collection = None
    
    def conectar(self, host, port, db_name, collection_name):
        try:
            self.client = MongoClient(host, port)
            self.db = self.client[db_name]
            self.collection = self.db[collection_name]
            return True
        except Exception as e:
            print("Error al conectar a MongoDB:", e)
            return False
    
    def insertar(self, documento):
        try:
            if self.collection!=None:
                result = self.collection.insert_one(documento)
                return result.inserted_id
        except Exception as e:
            print("Error al insertar documento:", e)
            return None
    
    def buscar(self, filtro):
        try:
            if self.collection!=None:
                return list(self.collection.find(filtro))
        except Exception as e:
            print("Error al buscar documentos:", e)
            return None
    
    def actualizar(self, filtro, nuevos_valores):
        try:
            if self.collection!=None:
                result = self.collection.update_many(filtro, {"$set": nuevos_valores})
                return result.modified_count
        except Exception as e:
            print("Error al actualizar documentos:", e)
            return 0
    
    def borrar(self, filtro):
        try:
            if self.collection!=None:
                result = self.collection.delete_many(filtro)
                return result.deleted_count
        except Exception as e:
            print("Error al borrar documentos:", e)
            return 0
    
    def desconectar(self):
        try:
            if self.client is not None:
                self.client.close()
        except Exception as e:
            print("Error al cerrar conexión:", e)

class ExcepcionIntegridad(Exception):
    def __init__(self, *args: object):
        super().__init__(*args)
            