# -*- coding: utf-8 -*-
import dask.dataframe as dd
import os
from dask.diagnostics import ProgressBar
from difflib import SequenceMatcher
import pandas as pd

# ------------------ Configurar rutas ------------------
BASE_DIR = os.path.dirname(os.path.dirname(__file__))
DATA_DIR = os.path.join(BASE_DIR, 'data')
OUTPUT_DIR = os.path.join(BASE_DIR, 'output')
os.makedirs(OUTPUT_DIR, exist_ok=True)

archivo1 = os.path.join(DATA_DIR, 'fuente1.csv')
archivo2 = os.path.join(DATA_DIR, 'fuente2.csv')
reporte_dif = os.path.join(OUTPUT_DIR, 'reporte_diferencias.csv')
reporte_dif_detallado = os.path.join(OUTPUT_DIR, 'reporte_diferencias_detallado.csv')
reporte_solo1 = os.path.join(OUTPUT_DIR, 'solo_fuente1.csv')
reporte_solo2 = os.path.join(OUTPUT_DIR, 'solo_fuente2.csv')

# ------------------ Validar archivos ------------------
if not os.path.exists(archivo1) or not os.path.exists(archivo2):
    raise FileNotFoundError("Uno o ambos archivos fuente no existen.")

# ------------------ Leer archivos CSV ------------------
df1 = dd.read_csv(archivo1, dtype=str, encoding="latin1")
df2 = dd.read_csv(archivo2, dtype=str, encoding="latin1")
df1 = df1.repartition(npartitions=100)
df2 = df2.repartition(npartitions=100)

# ------------------ Validar columnas ------------------
campos = ['nombre1', 'nombre2', 'apellido1', 'apellido2',
          'lugar_nacimiento', 'departamento_nacimiento', 'pais_nacimiento',
          'fecha_nacimiento', 'fecha_registro', 'fecha_actualizacion']

columnas_requeridas = ['cedula'] + campos
for df, nombre in [(df1, "fuente1"), (df2, "fuente2")]:
    for col in columnas_requeridas:
        if col not in df.columns:
            raise ValueError(f"Falta la columna '{col}' en {nombre}")

# ------------------ Merge por cédula ------------------
df_merge = df1.merge(df2, on='cedula', how='inner', suffixes=('_f1', '_f2'))

# ------------------ Rellenar vacíos y limpiar ------------------
for campo in campos:
    df_merge[f"{campo}_f1"] = df_merge[f"{campo}_f1"].fillna("").astype(str).str.strip()
    df_merge[f"{campo}_f2"] = df_merge[f"{campo}_f2"].fillna("").astype(str).str.strip()

# ------------------ Funciones de comparación ------------------
def calcular_similitud(str1, str2):
    return round(SequenceMatcher(None, str1, str2).ratio() * 100, 2)

def detectar_diferencias(row):
    difs = []
    for campo in campos:
        val1 = row.get(f"{campo}_f1", "")
        val2 = row.get(f"{campo}_f2", "")
        if val1 != val2:
            similitud = calcular_similitud(val1, val2)
            difs.append(f"{campo}: {val1} <> {val2} (similitud: {similitud}%)")
    return "; ".join(difs) if difs else None

def diferencias_detalladas(row):
    detalles = []
    for campo in campos:
        val1 = row.get(f"{campo}_f1", "")
        val2 = row.get(f"{campo}_f2", "")
        if val1 != val2:
            similitud = calcular_similitud(val1, val2)
            diferencia = f"{val1} <> {val2}"
            detalles.append({
                "cedula": row["cedula"],
                "tipo_diferencia": campo,
                "diferencia": diferencia,
                "porcentaje": similitud
            })
    return detalles

# ------------------ Aplicar comparación ------------------
df_merge['diferencias'] = df_merge.map_partitions(
    lambda df: df.apply(detectar_diferencias, axis=1),
    meta=('diferencias', 'object')
)

diferencias_expandibles = df_merge.map_partitions(
    lambda df: df.apply(diferencias_detalladas, axis=1),
    meta=('detalles', 'object')
)

# ------------------ Generar salidas ------------------
with ProgressBar():
    df_dif = df_merge[df_merge['diferencias'].notnull()][['cedula', 'diferencias']]
    df_dif.to_csv(reporte_dif.replace('.csv', '_*.csv'), index=False, single_file=True)

    listas = diferencias_expandibles.compute()
    diferencias_planas = []
    for sublist in listas:
        if sublist:
            diferencias_planas.extend(sublist)

    df_detalles = pd.DataFrame(diferencias_planas)
    df_detalles.to_csv(reporte_dif_detallado, index=False, encoding="utf-8")

# ------------------ Registros únicos ------------------
cedulas_f2 = df2[['cedula']].drop_duplicates()
solo_f1 = df1.merge(cedulas_f2, on='cedula', how='left', indicator=True)
solo_f1 = solo_f1[solo_f1['_merge'] == 'left_only'].drop('_merge', axis=1)

cedulas_f1 = df1[['cedula']].drop_duplicates()
solo_f2 = df2.merge(cedulas_f1, on='cedula', how='left', indicator=True)
solo_f2 = solo_f2[solo_f2['_merge'] == 'left_only'].drop('_merge', axis=1)

with ProgressBar():
    solo_f1.to_csv(reporte_solo1.replace('.csv', '_*.csv'), index=False, single_file=True)
    solo_f2.to_csv(reporte_solo2.replace('.csv', '_*.csv'), index=False, single_file=True)

print(f"Diferencias (resumen) guardadas en: {reporte_dif}")
print(f"Diferencias (detalladas) guardadas en: {reporte_dif_detallado}")
print(f"Registros unicos de fuente1 guardados en: {reporte_solo1}")
print(f"Registros unicos de fuente2 guardados en: {reporte_solo2}")
