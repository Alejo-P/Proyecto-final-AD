import mysql.connector, random, string
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
    
    def Consulta(self, SQL_query):
        """_Consulta SQL_

        Args:
            SQL_query (_str_): _Consulta que se desea realizar a la base de datos_

        Raises:
            ValueError: _Si la sentencia ingresada no pertenece a una sentencia de consulta "SELECT"_

        Returns:
            str: _ID de la consulta_
        """
        self._find=False
        if str(SQL_query).startswith("SELECT") or str(SQL_query).startswith("SHOW") or str(SQL_query).startswith("DESCRIBE"):
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
            raise ValueError("La sentencia {} no es una sentencia de consulta valida".format(SQL_query))
    
    def Modificar(self, SQL_query):
        """_Modificacion de registros en la base de datos_

        Args:
            SQL_query (_str_): _Sentencia de modificacion de registros_

        Raises:
            ValueError: _Si la sentencia ingresada no pertenece a una sentencia
            \n\t\t\tDML: "INSERT", "UPDATE", "DELETE"
            \n\t\t\tDDL: "ALTER", "DROP", "CREATE"_

        Returns:
            int: _Numero de filas afectadas por la consulta_
        """
        self._find=False
        self._sentenciasSQL=["INSERT", "UPDATE", "DELETE", "ALTER", "DROP", "CREATE"]
        try:
            for sentencia in self._sentenciasSQL:
                if str(SQL_query).startswith(sentencia):
                    self._ejecuion=self._cursor.execute(SQL_query)
                    self._conexion.commit()
                    return self._ejecuion
            if not self._find:
                raise ValueError("La sentencia {} no es una sentencia de consulta valida".format(SQL_query))
        except mysql.connector.Error as e:
            print("Error:", e)
    
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
            