# -*- coding: utf-8 -*-
import pandas as pd
import random
from faker import Faker
from pathlib import Path

fake = Faker('es_CO')
random.seed(42)
Faker.seed(42)

NUM_REGISTROS = 1_000_000
output_dir = Path("data")
output_dir.mkdir(exist_ok=True)

def generar_datos(base=0):
    data = []
    for i in range(NUM_REGISTROS):
        cedula = f"{base + i:010d}"
        nombre1 = fake.first_name()
        nombre2 = fake.first_name() if random.random() < 0.7 else ""
        apellido1 = fake.last_name()
        apellido2 = fake.last_name()
        lugar = fake.city()
        data.append([cedula, nombre1, nombre2, apellido1, apellido2, lugar])
    return pd.DataFrame(data, columns=["cedula", "nombre1", "nombre2", "apellido1", "apellido2", "lugar_nacimiento"])

print("Generando fuente1.csv...")
df1 = generar_datos(base=1000000000)
df1.to_csv(output_dir / "fuente1.csv", index=False, encoding="latin1")

print("Generando fuente2.csv con algunas diferencias...")
df2 = df1.copy()

# Introducir algunas diferencias artificiales
for i in range(0, NUM_REGISTROS, 10000):
    df2.loc[i, "nombre1"] = fake.first_name()
    df2.loc[i, "lugar_nacimiento"] = fake.city()

df2.to_csv(output_dir / "fuente2.csv", index=False, encoding="latin1")

print("Archivos de ejemplo generados")
