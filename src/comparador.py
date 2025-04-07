# -*- coding: utf-8 -*-
import dask.dataframe as dd
import os

# Configurar rutas
BASE_DIR = os.path.dirname(os.path.dirname(__file__))
DATA_DIR = os.path.join(BASE_DIR, 'data')
OUTPUT_DIR = os.path.join(BASE_DIR, 'output')
os.makedirs(OUTPUT_DIR, exist_ok=True)

archivo1 = os.path.join(DATA_DIR, 'fuente1.csv')
archivo2 = os.path.join(DATA_DIR, 'fuente2.csv')
reporte_dif = os.path.join(OUTPUT_DIR, 'reporte_diferencias.csv')
reporte_solo1 = os.path.join(OUTPUT_DIR, 'solo_fuente1.csv')
reporte_solo2 = os.path.join(OUTPUT_DIR, 'solo_fuente2.csv')

# Leer archivos CSV
df1 = dd.read_csv(archivo1, dtype=str, encoding="latin1")
df2 = dd.read_csv(archivo2, dtype=str, encoding="latin1")

# ----------- Coincidencias por cédula (inner join) -----------
df_merge = df1.merge(df2, on='cedula', how='inner', suffixes=('_f1', '_f2'))

# Campos a comparar
campos = ['nombre1', 'nombre2', 'apellido1', 'apellido2', 'lugar_nacimiento']
# Rellenar valores faltantes en los campos comparados
for campo in campos:
    df_merge[f"{campo}_f1"] = df_merge[f"{campo}_f1"].fillna("").astype(str).str.strip()
    df_merge[f"{campo}_f2"] = df_merge[f"{campo}_f2"].fillna("").astype(str).str.strip()

# Función para detectar diferencias campo a campo
def detectar_diferencias(row):
    diferencias = {}
    for campo in campos:
        val1 = row.get(f"{campo}_f1")
        val2 = row.get(f"{campo}_f2")
        if val1 != val2:
            diferencias[campo] = f"{val1} <> {val2}"
    return diferencias or None

# Aplicar comparación
df_merge['diferencias'] = df_merge.map_partitions(
    lambda df: df.apply(detectar_diferencias, axis=1),
    meta=('diferencias', 'object')
)

# Filtrar con diferencias
df_dif = df_merge[df_merge['diferencias'].notnull()][['cedula', 'diferencias']]
df_dif.compute().to_csv(reporte_dif, index=False)
print(f"? Diferencias guardadas en: {reporte_dif}")

# ----------- Solo en fuente1 -----------
cedulas_f2 = df2[['cedula']].drop_duplicates()
solo_f1 = df1.merge(cedulas_f2, on='cedula', how='left', indicator=True)
solo_f1 = solo_f1[solo_f1['_merge'] == 'left_only'].drop('_merge', axis=1)
solo_f1.compute().to_csv(reporte_solo1, index=False)
print(f"? Registros solo en fuente1 guardados en: {reporte_solo1}")

# ----------- Solo en fuente2 -----------
cedulas_f1 = df1[['cedula']].drop_duplicates()
solo_f2 = df2.merge(cedulas_f1, on='cedula', how='left', indicator=True)
solo_f2 = solo_f2[solo_f2['_merge'] == 'left_only'].drop('_merge', axis=1)
solo_f2.compute().to_csv(reporte_solo2, index=False)
print(f"? Registros solo en fuente2 guardados en: {reporte_solo2}")
