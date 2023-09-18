import psycopg2
import pandas as pd
import os
from dotenv import load_dotenv

# Cargar las variables de entorno desde el archivo .env
load_dotenv()

# Obtener la contraseña de la base de datos desde las variables de entorno
DB_PASSWORD = os.getenv('DB_PASSWORD')
HOST = "localhost"
PORT = os.getenv('PORT')

# Conexión a la base de datos
conn = psycopg2.connect(database="postgres", user="postgres", password=DB_PASSWORD, host=HOST, port=PORT)
cursor = conn.cursor()

# Carga de datos desde el archivo CSV
data = pd.read_csv('./output/merged_titles.csv')

# Reemplazar los valores "NaT" por None en la columna 'date_added'
data['date_added'] = data['date_added'].where(data['date_added'].notnull(), None)

# Convertir la columna 'date_added' al tipo de dato datetime
data['date_added'] = pd.to_datetime(data['date_added'], errors='coerce')

# Insertar los datos en la tabla 'titles'
for index, row in data.iterrows():
    date_added = row['date_added'] if pd.notnull(row['date_added']) else None

    cursor.execute("""
        INSERT INTO titles (type, title, director, "cast", country, date_added, release_year, rating, duration, listed_in, description, platform)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """, (row['type'], row['title'], row['director'], row['cast'], row['country'], date_added, row['release_year'], row['rating'], row['duration'], row['listed_in'], row['description'], row['platform']))


# Insertar los datos en la tabla 'types'
types_list = data['type'].str.split(', ').explode().unique()
for type in types_list:
    cursor.execute("""
        INSERT INTO types (type_name)
        VALUES (%s)
    """, (type,))

# Insertar los datos en la tabla 'ratings'
ratings_list = data['rating'].str.split(', ').explode().unique()
for rating in ratings_list:
    cursor.execute("""
        INSERT INTO ratings (rating_name)
        VALUES (%s)
    """, (rating,))

# Insertar los datos en la tabla 'actors'
actors_list = data['cast'].str.split(', ').explode().unique()
for actor in actors_list:
    cursor.execute("""
        INSERT INTO actors (actor_name)
        VALUES (%s)
    """, (actor,))

# Obtener una lista única de directores
directors_list = data['director'].str.split(', ').explode().unique()

# Insertar los datos en la tabla 'directors'
for director in directors_list:
    cursor.execute("""
        INSERT INTO directors (director_name)
        VALUES (%s)
    """, (director,))


platforms_list = data['platform'].unique()
# Insertar los datos únicos en la tabla 'platforms'
for platform in platforms_list:
    cursor.execute("""
        INSERT INTO platforms (platform_name)
        VALUES (%s)
    """, (platform,))


# Insertar los datos en la tabla 'titles' y obtener listas únicas de genres y countries
genres_list = data['listed_in'].str.split(', ').explode().unique()

# Insertar los datos únicos en la tabla 'genres'
for genre in genres_list:
    cursor.execute("""
        INSERT INTO genres (genre_name)
        VALUES (%s)
    """, (genre,))


countries_list = data['country'].str.split(', ').explode().unique()
# Insertar los datos únicos en la tabla 'countries'
for country in countries_list:
    cursor.execute("""
        INSERT INTO countries (country_name)
        VALUES (%s)
    """, (country,))

# Guardar los cambios y cerrar la conexión
conn.commit()
cursor.close()
conn.close()
