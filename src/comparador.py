import dask.dataframe as dd
import os

# Configurar rutas (puedes parametrizar esto más adelante)
DATA_DIR = os.path.join(os.path.dirname(__file__), '..', 'data')
OUTPUT_DIR = os.path.join(os.path.dirname(__file__), '..', 'output')
os.makedirs(OUTPUT_DIR, exist_ok=True)

archivo1 = os.path.join(DATA_DIR, 'fuente1.csv')
archivo2 = os.path.join(DATA_DIR, 'fuente2.csv')
reporte_salida = os.path.join(OUTPUT_DIR, 'reporte_diferencias.csv')

# Leer archivos CSV
df1 = dd.read_csv(archivo1, dtype=str)
df2 = dd.read_csv(archivo2, dtype=str)

# Comparar por cedula
df_merge = df1.merge(df2, on='cedula', how='inner', suffixes=('_f1', '_f2'))

# Campos a comparar
campos = ['nombre1', 'nombre2', 'apellido1', 'apellido2', 'lugar_nacimiento', 'vigencia']

# Detectar diferencias por fila
def detectar_diferencias(row):
    diferencias = {}
    for campo in campos:
        val1 = row.get(f"{campo}_f1")
        val2 = row.get(f"{campo}_f2")
        if val1 != val2:
            diferencias[campo] = f"{val1} <> {val2}"
    return diferencias or None

# Aplicar
df_merge['diferencias'] = df_merge.map_partitions(
    lambda df: df.apply(detectar_diferencias, axis=1),
    meta=('diferencias', 'object')
)

# Filtrar registros con diferencias
df_dif = df_merge[df_merge['diferencias'].notnull()][['cedula', 'diferencias']]

# Guardar reporte
df_dif.compute().to_csv(reporte_salida, index=False)
print(f"Reporte generado en: {reporte_salida}")