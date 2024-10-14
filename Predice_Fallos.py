
from joblib import load


def predecir_fallo(nueva_medida):
    modelo_cargado = load('modelo_fallos_hervido.joblib')
    prediccion = modelo_cargado.predict([nueva_medida])
    if prediccion[0] == 1:
        return "Fallo detectado."
    else:
        return "Sin fallos."
