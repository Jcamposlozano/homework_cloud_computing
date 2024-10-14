import pandas as pd
import numpy as np
import random
import time
from sklearn.model_selection import train_test_split
from sklearn.tree import DecisionTreeClassifier
from sklearn.metrics import classification_report, confusion_matrix
from joblib import dump  

def generar_fallos(temperatura, presion, pH):
    fallo = 0
    if temperatura < 90 or temperatura > 105:
        fallo = 1
    elif presion < 0.5 or presion > 1.5:
        fallo = 1
    elif pH < 4.5 or pH > 6.0:
        fallo = 1
    return fallo

def simular_datos_hervido(duracion_horas=1):
    datos = []
    tiempo_total = duracion_horas * 3600 
    tiempo_medido = 0

    while tiempo_medido < tiempo_total:
        temperatura = random.uniform(100, 102)  
        presion = random.uniform(0.8, 1.2)  
        pH = random.uniform(5.0, 5.5)  
        densidad = random.uniform(1.040, 1.060)
        tasa_adicion_lupulo = random.uniform(0, 5)
        oxigeno_disuelto = max(0, random.uniform(0, 0.5))
        evaporacion = random.uniform(0, 0.1)
        
        fallo = generar_fallos(temperatura, presion, pH)
        
        datos.append([
            tiempo_medido, 
            temperatura, 
            presion, 
            pH, 
            densidad, 
            tasa_adicion_lupulo, 
            oxigeno_disuelto, 
            evaporacion, 
            fallo
        ])
        
        tiempo_medido += 2  
    columnas = ["Tiempo (s)", "Temperatura (°C)", "Presión (bar)", "pH", 
                "Densidad", "Tasa de Adición de Lúpulo (g)", 
                "Oxígeno Disuelto (ppm)", "Evaporación (litros)", "Fallo"]
    
    df = pd.DataFrame(datos, columns=columnas)
    return df


df_datos = simular_datos_hervido()
print(df_datos.head())  


X = df_datos.drop(columns=["Tiempo (s)", "Fallo"])  
y = df_datos["Fallo"]  


X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)


modelo = DecisionTreeClassifier(random_state=42)
modelo.fit(X_train, y_train)

y_pred = modelo.predict(X_test)

print(confusion_matrix(y_test, y_pred))
print(classification_report(y_test, y_pred))

dump(modelo, 'modelo_fallos_hervido.joblib')
print("Modelo guardado como 'modelo_fallos_hervido.joblib'")
