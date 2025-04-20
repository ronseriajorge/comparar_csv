from limpieza_utils import (
    quitar_espacios,
    convertir_minusculas,
    normalizar_cedula,
    convertir_a_int,
    reemplazar_caracter,
    formatear_fecha,
    asignar_encabezado
)

def main():
    ruta_csv = 'data/original/archivo1.csv'
    
    # Si el archivo NO tiene encabezado, usa header=None
    df = dd.read_csv(ruta_csv, sep='\t', assume_missing=True, header=None)

    # Asignar encabezado manualmente
    nombres_columnas = ['nombre', 'cedula', 'edad', 'correo', 'fecha_nacimiento']
    df = asignar_encabezado(df, nombres_columnas)

    # Configuración de limpieza por columna
    config_limpieza = {
        'nombre': [quitar_espacios, convertir_minusculas],
        'cedula': [quitar_espacios, normalizar_cedula],
        'edad': [quitar_espacios, convertir_a_int],
        'correo': [quitar_espacios, convertir_minusculas],
        'fecha_nacimiento': [
            lambda col: reemplazar_caracter(col, '/', '-'),
            lambda col: formatear_fecha(col, '%d-%m-%Y')
        ],
    }

    df_limpio = aplicar_transformaciones(df, config_limpieza)

    df_limpio.to_csv(
        'data/limpio/archivo1_limpio_*.csv',
        index=False,
        sep='\t',
        single_file=True
    )
    print("Archivo limpio guardado con éxito.")
