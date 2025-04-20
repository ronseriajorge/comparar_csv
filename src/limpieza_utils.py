# limpieza_utils.py

import dask.dataframe as dd

def quitar_espacios(col):
    """Elimina espacios al inicio y final de cada celda"""
    return col.str.strip()

def convertir_minusculas(col):
    """Convierte a minúsculas"""
    return col.str.lower()

def convertir_mayusculas(col):
    """Convierte a mayúsculas"""
    return col.str.upper()

def eliminar_caracteres_especiales(col, caracteres='[^a-zA-Z0-9\s]'):
    """Elimina caracteres especiales por regex"""
    return col.str.replace(caracteres, '', regex=True)

def convertir_a_float(col):
    """Convierte la columna a float, reemplazando errores por NaN"""
    return col.astype(float)

def convertir_a_int(col):
    """Convierte la columna a int, permitiendo NaNs"""
    return col.astype('Int64')  # tipo entero que soporta nulos

def normalizar_cedula(col):
    """Elimina puntos y ceros a la izquierda en una cédula"""
    return col.str.replace(r'\.', '', regex=True).str.lstrip('0')

def reemplazar_valores_nulos(col, valor="DESCONOCIDO"):
    """Reemplaza valores nulos por un valor dado"""
    return col.fillna(valor)

def reemplazar_caracter(col, viejo, nuevo):
    """
    Reemplaza un carácter (o patrón) por otro en una columna de tipo string.
    Ejemplo: reemplazar '/' por '-'
    """
    return col.str.replace(viejo, nuevo, regex=True)

def formatear_fecha(col, formato='%d/%m/%Y'):
    """
    Convierte un campo de texto a datetime según el formato indicado.
    """
    return dd.to_datetime(col, format=formato, errors='coerce')

def asignar_encabezado(df, nombres_columnas):
    """
    Asigna un nuevo encabezado al DataFrame usando la lista de nombres proporcionada.
    """
    df.columns = nombres_columnas
    return df