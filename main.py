import threading
import random
import time
import queue
import psycopg2

from Predice_Fallos import predecir_fallo

# Configuración de la conexión a PostgreSQL
DB_CONFIG = {
    'dbname': 'postgres',
    'user': 'beer_admin',
    'password': 'beer_pass',
    'host': 'localhost',
    'port': 5432
}

# Simulación de stream usando una cola
evento_parada = threading.Event()
data_stream = queue.Queue()

class ContenedorFermentacion(threading.Thread):
    def __init__(self, id_contenedor, nivel_mosto_inicial):
        super().__init__()
        self.id_contenedor = id_contenedor
        self.nivel_mosto = nivel_mosto_inicial
        self.temperatura = random.uniform(100, 102)
        self.presion = random.uniform(0.8, 1.2)
        self.estado_activo = self.nivel_mosto > 0

    def run(self):
        if not self.estado_activo:
            print(f"[Contenedor-{self.id_contenedor}] Apagado: Sin nivel de mosto.")
            return

        print(f"[Contenedor-{self.id_contenedor}] -> Iniciando monitoreo. Nivel de mosto: {self.nivel_mosto} litros.")

        while self.estado_activo and not evento_parada.is_set():
            self.temperatura += random.uniform(-0.5, 0.5)
            self.presion += random.uniform(-0.05, 0.05)

            nueva_medida = {
                'contenedor_id': self.id_contenedor,
                'temperatura': self.temperatura,
                'presion': self.presion,
                'nivel_mosto': self.nivel_mosto,
                'timestamp': time.time()
            }

            # Simular envío al stream
            print(f"[Contenedor-{self.id_contenedor}] Enviando datos: {nueva_medida}")
            data_stream.put(nueva_medida)

            if self.nivel_mosto <= 0:
                self.estado_activo = False
                print(f"[Contenedor-{self.id_contenedor}] Apagado: Sin nivel de mosto.")
            time.sleep(2)

        print(f"[Contenedor-{self.id_contenedor}] -> Monitoreo detenido.")



def get_db_connection():
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        return conn
    except psycopg2.OperationalError as e:
        print(f"[Error] No se pudo conectar a PostgreSQL: {e}")
        return None


def procesar_stream():
    conn = get_db_connection()
    if not conn:
        return

    cursor = conn.cursor()
    insert_query = """
        INSERT INTO public.datos_calderas (
            contenedor_id, temperatura, presion, nivel_mosto, timestamp
        ) VALUES (%s, %s, %s, %s, %s)
    """

    while not evento_parada.is_set() or not data_stream.empty():
        try:
            evento = data_stream.get(timeout=1)

            if not isinstance(evento, dict):
                print(f"[Error] Evento inválido recibido: {evento}")
                continue

            # Preparar los datos para insertar
            data = (
                evento['contenedor_id'],
                evento['temperatura'],
                evento['presion'],
                evento['nivel_mosto'],
                time.strftime('%Y-%m-%d %H:%M:%S', time.gmtime(evento['timestamp']))
            )

            # Insertar en la base de datos
            cursor.execute(insert_query, data)
            conn.commit()

            print(f"[Procesador] Datos insertados para contenedor {evento['contenedor_id']}")

        except queue.Empty:
            pass
        except psycopg2.Error as e:
            print(f"[Error] Fallo al insertar datos: {e}")
            conn.rollback()

    cursor.close()
    conn.close()
    print("[Procesador] Conexión a PostgreSQL cerrada.")

def iniciar_monitoreo_contenedores(contenedores_data, duracion_minutos):
    contenedores = [
        ContenedorFermentacion(id_c, nivel) 
        for id_c, nivel in contenedores_data.items()
    ]

    # Iniciar los hilos de contenedores
    for contenedor in contenedores:
        contenedor.start()

    # Iniciar el procesador del stream
    procesador = threading.Thread(target=procesar_stream)
    procesador.start()

    # Esperar la duración del monitoreo
    time.sleep(duracion_minutos * 60)
    evento_parada.set()

    # Esperar a que todos los hilos terminen
    for contenedor in contenedores:
        contenedor.join()
    procesador.join()

if __name__ == "__main__":
    contenedores_data = {
        1: 500.0,  # Contenedor 1 con 500 litros
        2: 450.0,  # Contenedor 2 con 450 litros
        3: 0.0,    # Contenedor 3 vacío (estará apagado)
        4: 600.0,  # Contenedor 4 con 600 litros
        5: 300.0,  # Contenedor 5 con 300 litros
        6: 700.0,  # Contenedor 6 con 700 litros
        7: 0.0,    # Contenedor 7 vacío (estará apagado)
        8: 550.0,  # Contenedor 8 con 550 litros
        9: 400.0,  # Contenedor 9 con 400 litros
        10: 500.0  # Contenedor 10 con 500 litros
    }
    duracion_minutos = 1

    iniciar_monitoreo_contenedores(contenedores_data, duracion_minutos)
