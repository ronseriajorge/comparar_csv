# -*- coding: utf-8 -*-
import pandas as pd
import random
from faker import Faker
from pathlib import Path
from tqdm import tqdm
from datetime import datetime

# Inicialización
fake = Faker('en_US')
random.seed(42)
Faker.seed(42)

NUM_REGISTROS = 6_000_000
CHUNK_SIZE = 1_000_000
REGISTROS_A_ELIMINAR = 10000
REGISTROS_EXTRA = 1000
output_dir = Path("data")
output_dir.mkdir(exist_ok=True)

def generar_datos(base=0, cantidad=1_000_000):
    data = []
    for i in range(cantidad):
        cedula = f"{base + i:010d}"
        nombre1 = fake.first_name()
        nombre2 = fake.first_name() if random.random() < 0.7 else ""
        apellido1 = fake.last_name()
        apellido2 = fake.last_name()
        lugar = fake.city()
        departamento = fake.state()
        pais = fake.current_country()
        fecha_nacimiento = fake.date_of_birth(minimum_age=18, maximum_age=65).strftime('%Y-%m-%d')
        fecha_registro = fake.date_between(start_date='-5y', end_date='-1y')
        fecha_actualizacion = fake.date_between(start_date=fecha_registro, end_date='today')

        data.append([
            cedula, nombre1, nombre2, apellido1, apellido2,
            lugar, departamento, pais,
            fecha_nacimiento, fecha_registro.strftime('%Y-%m-%d'), fecha_actualizacion.strftime('%Y-%m-%d')
        ])
    
    columnas = [
        "cedula", "nombre1", "nombre2", "apellido1", "apellido2",
        "lugar_nacimiento", "departamento_nacimiento", "pais_nacimiento",
        "fecha_nacimiento", "fecha_registro", "fecha_actualizacion"
    ]
    return pd.DataFrame(data, columns=columnas)

# Generar fuente1.csv
print("Generando fuente1.csv...")
for i in tqdm(range(0, NUM_REGISTROS, CHUNK_SIZE), desc="fuente1.csv"):
    chunk_df = generar_datos(base=1000000000 + i, cantidad=CHUNK_SIZE)
    modo = 'w' if i == 0 else 'a'
    header = i == 0
    chunk_df.to_csv(output_dir / "fuente1.csv", mode=modo, index=False, encoding="latin1", header=header)

# Generar fuente2.csv con diferencias
print("Generando fuente2.csv con diferencias (modificados, eliminados, agregados)...")
for i in tqdm(range(0, NUM_REGISTROS, CHUNK_SIZE), desc="fuente2.csv"):
    df_chunk = pd.read_csv(output_dir / "fuente1.csv", skiprows=range(1, i + 1), nrows=CHUNK_SIZE, encoding="latin1")

    # MODIFICAR registros cada 10,000
    for j in range(0, len(df_chunk), 10000):
        df_chunk.loc[j, "nombre1"] = fake.first_name()
        df_chunk.loc[j, "lugar_nacimiento"] = fake.city()
        df_chunk.loc[j, "fecha_actualizacion"] = datetime.now().strftime('%Y-%m-%d')

    # ELIMINAR algunos registros
    df_chunk = df_chunk.drop(df_chunk.sample(n=REGISTROS_A_ELIMINAR, random_state=42).index)

    # AGREGAR nuevos registros con cédulas que no estén en fuente1
    base_nuevos = 2000000000 + i  # Base fuera del rango usado en fuente1
    nuevos_df = generar_datos(base=base_nuevos, cantidad=REGISTROS_EXTRA)

    df_chunk = pd.concat([df_chunk, nuevos_df], ignore_index=True)

    modo = 'w' if i == 0 else 'a'
    header = i == 0
    df_chunk.to_csv(output_dir / "fuente2.csv", mode=modo, index=False, encoding="latin1", header=header)

print("? Archivos generados con diferencias realistas.")
