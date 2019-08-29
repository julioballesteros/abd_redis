__author__ = 'Julio Ballesteros'

import pymongo
from pymongo import MongoClient
import json
import ssl
import geopy
import geojson
import datetime
import redis
import uuid
from threading import Thread
from time import sleep


def getCityGeoJSON(adress):
    """ Devuelve las coordenadas de una direccion a partir de un str de la direccion
    Argumentos:
        adress (str) -- Direccion
    Return:
        (str) -- GeoJSON
    """
    from geopy.geocoders import Nominatim
    geopy.geocoders.options.default_user_agent = "P1_abd.py"
    #Solucion error
    ctx = ssl.create_default_context()
    ctx.check_hostname = False
    ctx.verify_mode = ssl.CERT_NONE
    geopy.geocoders.options.default_ssl_context = ctx

    geolocator = Nominatim()
    location = geolocator.geocode(adress)

    # TODO
    # Devolver GeoJSON de tipo punto con la latitud y longitud almacenadas
    # en las variables location.latitude y location.longitude
    return geojson.Point([location.longitude, location.latitude])



class ModelCursor(object):
    """ Cursor para iterar sobre los documentos del resultado de una
    consulta. Los documentos deben ser devueltos en forma de objetos
    modelo.
    """

    def __init__(self, model_class, command_cursor):
        """ Inicializa ModelCursor
        Argumentos:
            model_class (class) -- Clase para crear los modelos del
            documento que se itera.
            command_cursor (CommandCursor) -- Cursor de pymongo
        """
        # TODO
        self.model_class = model_class
        self.command_cursor = command_cursor

    def next(self):
        """ Devuelve el siguiente documento en forma de modelo
        """
        if self.alive:
            object = self.command_cursor.next()
            return self.model_class(**object)


    @property
    def alive(self):
        """True si existen más modelos por devolver, False en caso contrario
        """
        # TODO
        return self.command_cursor.alive


class Model(object):
    """ Prototipo de la clase modelo
        Copiar y pegar tantas veces como modelos se deseen crear (cambiando
        el nombre Model, por la entidad correspondiente), o bien crear tantas
        clases como modelos se deseen que hereden de esta clase. Este segundo
        metodo puede resultar mas complejo
    """
    required_vars = []
    #admissible_vars contiene todas las requiered_vars
    admissible_vars = []
    geojson_vars = []
    db = None
    cache = None


    """
        Constructor de la instancia, comprueba que la nueva instancia
        tiene los argumentos necesarios y todos son admisibles
    """
    def __init__(self, **kwargs):
        # TODO
        self.var_mod = []
        self._id = None
        #El **kwards es un diccionario desempaquetado para pasarselo llamando a la funcion con **dict

        #Comprobar que tiene todos los argumentos necesarios ( estan los requiered_vars)
        if not all(elem in kwargs.keys() for elem in self.__class__.required_vars):
            raise ValueError('Se ha introducido una variable no valida.')

        #Comprobar que los demas argumentos son admisibles (estan en admissible_vars)
        if not all(elem in self.admissible_vars for elem in kwargs.keys()):
            raise ValueError('Se ha introducido una variable no valida.')

        #Convertir atributos a geojson
        for var in self.geojson_vars:
            if var in kwargs.keys():
                kwargs[var] = getCityGeoJSON(kwargs[var])

        #Crear el objeto
        self.__dict__.update(kwargs)


    """
        El metodo save guarda en la db los argumentos medificados,
        que se han apuntado en var_mod, si no tiene id se guarda todo
    """
    def save(self):
        # TODO

        #si no tiene id se guarda nuevo entero
        if self._id == None:
            print('adios')
            dicc = self.__dict__.copy()
            dicc.pop('_id')
            dicc.pop('var_mod')
            self._id = self.db.insert_one(dicc).inserted_id
            self.var_mod = []

        #comprobar las variables modificadas var_mod
        #actualizar en db con consulta update las variables modificadas
        else:
            if len(self.var_mod) > 0:
                dicc = {}
                for var in self.var_mod:
                    dicc[var] = self.__dict__[var]
                query = {'_id': self._id}
                self.db.update(query, dicc)
                self.var_mod = []



    """
        El metodo update actualiza la instancia del objeto con nuevos
        argumentos, los cuales tiene que comprobarse que son admisibles,
        y apuntar los argumentos modificados
    """
    def update(self, **kwargs):
        # TODO
        #El **kwards es un diccionario desempaquetado para pasarselo llamando a la funcion con **dict
        #comprobar que los kwards son admitidos (admissible_vars)
        if not all(elem in self.admissible_vars for elem in kwargs.keys()):
            raise ValueError('Se ha introducido una variable no valida.')

        #cambiar las variables indicadas
        self.__dict__.update(kwargs)

        # Convertir atributos a geojson
        for var in self.geojson_vars:
            if var in kwargs.keys():
                self.__dict__[var] = getCityGeoJSON(self.__dict__[var])

        #apuntar las variables cambiadas en var_mod
        for key in kwargs.keys():
            if key not in self.var_mod:
                self.var_mod.append(key)

    """
        Realiza consultas sobre la base de datos
    """
    @classmethod
    def query(cls, query):
        """ Devuelve un cursor de modelos
        """
        # TODO
        # cls() es el constructor de esta clase
        #Realizar la consulta
        cursor = cls.db.aggregate(query)
        #Crear el objeto cursor y devolverlo
        model_cursor = ModelCursor(cls, cursor)
        return model_cursor

    @classmethod
    def query_by_id(cls, id):
        # si el id se encuentra almacenado en la cache
        if cache.exists('UserID:' + id):
            # obtener objeto de la cache
            result = cache.get('UserID:' + id)
            # reiniciar tiempo en cache
            cache.expire('UserID:' + id, 86400)
            #Pasar string a diccionario
            result = json.loads(result)

        # si no
        else:
            # obtenerlo de mongodb
            query = {"_id": id}
            result = cls.db.find_one(query)
            #insertarlo en cache
            cache.set('UserID:' + id, json.dumps(result), ex=86400)
        # pasarlo a modelo
        return cls(**result)


    @classmethod
    def init_class(cls, db, cache, vars_path="model_vars.json"):
        """ Inicializa las variables de clase en la inicializacion del sistema.
        Argumentos:
            db (MongoClient) -- Conexion a la base de datos.
            vars_path (str) -- ruta al archivo con la definicion de variables
            del modelo.
        """
        # TODO
        # cls() es el constructor de esta clase

        #Conecta con las bbdd
        cls.db = db
        cls.cache = cache

        #Obtener los datos json del fichero
        with open(vars_path, 'r') as json_data:
            data = json.load(json_data)
            json_data.close()

        #Inicializar las variables required_vars, admissible_vars y geojson_vars
        for model_data in data:
            if cls.__name__ == model_data["model_name"]:
                cls.required_vars = model_data["required_vars"].copy()
                cls.admissible_vars = model_data["admissible_vars"].copy()
                cls.geojson_vars = model_data["geoJSON_vars"].copy()

"""
    Implementación de los modelos concretos heredando de la clase model
"""
class Client(Model):
    pass

class Product(Model):
    pass

class Purchase(Model):
    #permite actualizar la entrada de modo que se le asigne a cada producto el almacén más cercano al cliente.
    def allocate(self):
        pass

class Suplier(Model):
    pass


#Manejo de sesiones

def access_data(db, data):
#Inserta los datos en bbdd
    db.mset(data)

def login_new_session(db, username, password):
# hace login generando una nueva sesión
# devuelve el valor de los privilegios y el token asignado
# en caso de login satisfactorio y - 1 en caso contrario

    #obtener usuario correspondiente
    if not db.exists('Username:' + username): return -1
    user = db.get('Username:' + username)

    #comprobar que coincide la contraseña
    if not user['password'] == password: return -1

    #crear la sesion
    token = str(uuid.uuid3(uuid.uuid4(), user))
    user['token'] = token
    db.set('Session:' + token, user, ex= 2592000)


    return user['privileges'], token

def login(db, token):
# un método para hacer login mediante un token de sesión Devolverá el valor del campo
# privilegios asignados en caso de login satisfactorio y -1 en caso contrario;

    #Comprueba que la sesion esta iniciada
    if not db.exists('Session:' + token): return -1

    #Si existe se obtiene y se devuelven los privilegios
    session = db.get('Session' + token)
    return session['privileges']


#Empaquetado

def enqueue_purchase(db, purchase):
# añade la compra enviada a la cola
    db.rpush('purchases', purchase)



def master_service(db):
# proceso principal con espera indefinida,
# espera a que llegue una compra a la cola
# para empaquetarla y crear un nuevo proceso
    pid = 1
    while True:
        purchase = db.blpop('purchases')
        thread = Thread(target=slave_service, args=(db, pid + 1))
        thread.start()
        package_process(db, purchase, pid)


def slave_service(db, pid):
# servicio secundario, hace lo mismo que el principal
# pero despues de un min esperando muere
    while True:
        purchase = db.blpop('purchases', timeout=60)
        if purchase == None:
            break
        thread = Thread(target=slave_service, args=(db, pid + 1))
        thread.start()
        package_process(db, purchase, pid)
    print(str(pid) + 'terminated')

def package_process(db, purchase, pid):
# metodo no implementado que representa el empaquetamiento de una compra
# implementa un control estadistico sobre las empaquetaciones
    print('Packaging with thread ' + str(pid))
    sleep(15)

    day = datetime.datetime.now().date()
    # incrementa el numero de paquetes procesados por este proceso este dia y este mes
    db.zincrby('Day_count:' + str(day.year) + ':' + str(day.month) + ':' + str(day.day), 1, pid)
    db.zincrby('Month_count:' + str(day.year) + ':' + str(day.month), 1, pid)


def year_count(db, year):
    months = []
    for i in range(12):
        months.append('Month_count:'+ str(year) + ':' + str(i + 1))

    db.zinterstore('Year_count:' + str(year), months, aggregate='MAX')


if __name__ == '__main__':
    # TODO

    #Establecemos la conexion con la bd
    #client = MongoClient('localhost', 27017)
    #db = client.test_db
    cache = redis.Redis(host='localhost', port=6379, db=0)
    cache.config_set('maxmemory', '150mb')
    cache.config_set('maxmemory-policy', 'volatile-ttl')
    '''
    #Inicializamos los modelos
    Client.init_class(db.clients, cache)
    Product.init_class(db.products, cache)
    Purchase.init_class(db.purchases, cache)
    Suplier.init_class(db.supliers, cache)

    #Creamos los indices
    db.clients.create_index([('mail_address', pymongo.GEOSPHERE)])
    db.purchases.create_index([('mail_address', pymongo.GEOSPHERE)])
    db.supliers.create_index([('warehouse_address', pymongo.GEOSPHERE)])
    '''
    #Inicializamos el proceso de empaquetamiento
    thread = Thread(target=master_service, args=(cache,))
    thread.setDaemon(True)
    thread.start()

    for i in range(5):
        sleep(2)
        enqueue_purchase(cache, i)

    sleep(120)


