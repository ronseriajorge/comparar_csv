# comparar_csv

Herramienta para comparar archivos CSV de gran tamaño de forma eficiente.  
Permite detectar coincidencias y diferencias entre registros, ideal para grandes volúmenes de datos (millones de filas), como cruces de información entre bases de datos.

---

## ?? Características

- Comparación eficiente de archivos CSV por columna clave (ej: cédula).
- Detección de diferencias en campos específicos.
- Generación de reportes de coincidencias y discrepancias.
- Soporte para archivos con millones de registros usando `dask`.

---

## ?? Requisitos

- Python 3.8 o superior

---

## ?? Instalación

### 1. Crear entorno virtual (recomendado)

```bash
python -m venv env
source env/bin/activate        # En Windows: env\Scripts\activate

```
### 2. Instalar librerias en una maquina sin internet 

```bash
cd paquetes_offline
pip install --no-index --find-links=. -r requirements.txt

```
### 3. Instalar librerias en una maquina con internet 

```bash
pip install -r requirements.txt

```