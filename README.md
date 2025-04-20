# comparar_csv

Herramienta para comparar archivos CSV de gran tama�o de forma eficiente.  
Permite detectar coincidencias y diferencias entre registros, ideal para grandes vol�menes de datos (millones de filas), como cruces de informaci�n entre bases de datos.

---

## ?? Caracter�sticas

- Comparaci�n eficiente de archivos CSV por columna clave (ej: c�dula).
- Detecci�n de diferencias en campos espec�ficos.
- Generaci�n de reportes de coincidencias y discrepancias.
- Soporte para archivos con millones de registros usando `dask`.

---

## ?? Requisitos

- Python 3.8 o superior

---

## ?? Instalaci�n

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