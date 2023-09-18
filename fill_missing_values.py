import os
import requests
import pandas as pd
from dotenv import load_dotenv

dotenv_path = os.path.join(os.path.dirname(__file__), '.env')
load_dotenv(dotenv_path)

API_KEY = os.getenv('API_TMDB_KEY')
BASE_URL = "https://api.themoviedb.org/3/"


# Movies
def get_movie_details_by_title(title):
    url = BASE_URL + f"search/movie?api_key={API_KEY}&query={title}&include_adult=false&language=en-US&page=1"
    response = requests.get(url)
    return response

def get_movie_details_by_id(title_id):
    url = BASE_URL + f"movie/{title_id}?api_key={API_KEY}&language=en-US"
    response = requests.get(url)
    return response

def get_movie_cast_and_director_by_id(title_id):
    url = BASE_URL + f"movie/{title_id}/credits?api_key={API_KEY}&language=en-US"
    response = requests.get(url)
    return response

# TV Shows
def get_tvshow_details_by_title(title):
    url = BASE_URL + f"search/tv?api_key={API_KEY}&query={title}&include_adult=false&language=en-US&page=1"
    response = requests.get(url)
    return response

def get_tvshow_details_by_id(tvshow_id):
    url = BASE_URL + f"tv/{tvshow_id}?api_key={API_KEY}&language=en-US"
    response = requests.get(url)
    return response

def get_tvshow_cast_and_director_by_id(title_id):
    url = BASE_URL + f"tv/{title_id}/credits?api_key={API_KEY}&language=en-US"
    response = requests.get(url)
    return response



def check_null_values(df):
    if (len(df) == 0):
        print("No null values found in the column.")

    return df



def fill_cast(df):
    # Verificamos si hay valores nulos
    null_rows = df[df['cast'].isnull()]

    # Se hace un reset de los index para poder trabajar con ellos correctamente
    null_rows.reset_index(drop=True, inplace=True)

    check_null_values(df)

    for index, row in null_rows.iterrows():
        title = row['title'].replace(' ', '+') # Si tiene espacios, hay que reemplazar esos espacios por un '+'

        if row['type'] == 'Movie':
            response = get_movie_details_by_title(title)
        else:
            response = get_tvshow_details_by_title(title)

        if response.status_code == 200:
            data = response.json()

            # Verificamos si se encontraron resultados
            if data['total_results'] > 0:
                # El ID del titulo
                title_id = data['results'][0]['id']

        if (row['type'] == 'Movie'):
            response = get_movie_cast_and_director_by_id(title_id)
        else:
            response = get_tvshow_cast_and_director_by_id(title_id)

        if response.status_code == 200:
            data = response.json()
            # Busco en el dataframe original el show_id y obtengo su index
            index_df = df[df['show_id'] == row['show_id']].index[0]

            if len(data['cast']) > 0:
                cast = [item['name'] for item in data['cast']]
                cast_string = ', '.join(cast)
                # Lleno la columna 'cast' en el index que obtuvimos
                df.at[index_df, 'cast'] = cast_string

    return df



def fill_director(df):

    null_rows = df[df['director'].isnull()]

    null_rows.reset_index(drop=True, inplace=True)

    check_null_values(df)

    for index, row in null_rows.iterrows():
        title = row['title'].replace(' ', '+') # Si tiene espacios, hay que reemplazar esos espacios por un '+'

        if row['type'] == 'Movie':
            response = get_movie_details_by_title(title)
        else:
            response = get_tvshow_details_by_title(title)

        if response.status_code == 200:
            data = response.json()
            # Verificar si se encontraron resultados y si hay información de país disponible
            if data['total_results'] > 0:
                title_id = data['results'][0]['id']    

        if (row['type'] == 'Movie'):
            response = get_movie_cast_and_director_by_id(title_id)
        else:
            response = get_tvshow_cast_and_director_by_id(title_id)

        if response.status_code == 200:
            data = response.json()
            index_df = df[df['show_id'] == row['show_id']].index[0]

            if (len(data['crew']) > 0):
                directors = [item['name'] for item in data['crew'] if item['known_for_department'] == 'Directing']
                directors_string = ', '.join(directors)
                df.at[index_df, 'director'] = directors_string
                
    return df


def fill_country(df):
    # Obtener las filas con valores NaN en la columna 'country'
    null_rows = df[df['country'].isnull()]

    null_rows.reset_index(drop=True, inplace=True)

    check_null_values(null_rows)

    for index, row in null_rows.iterrows():
        title = row['title'].replace(' ', '+') # Si tiene espacios, hay que reemplazar esos espacios por un '+'

        if row['type'] == 'Movie':
            response = get_movie_details_by_title(title)
        else:
            response = get_tvshow_details_by_title(title)

        if response.status_code == 200:
            data = response.json()
            if data['total_results'] > 0:
                title_id = data['results'][0]['id']

                if row['type'] == 'Movie':
                    response = get_movie_details_by_id(title_id)
                else: 
                    response = get_tvshow_details_by_id(title_id)

                if response.status_code == 200:
                    data = response.json()
                    
                    # En el dataframe original siempre esta 'United States'. Para no crear confusiones.
                    for item in data['production_countries']:
                        if item['name'] == 'United States of America':
                            item['name'] = 'United States'

                    countries_names = [item['name'] for item in data['production_countries']]
                    countries_string = ', '.join(countries_names)

                    index_df = df[df['show_id'] == row['show_id']].index[0]
                    df.at[index_df, 'country'] = countries_string
    return df

def fill_genres(df):
    # Obtener las filas con valores NaN en la columna 'listed_in'
    null_rows = df[df['listed_in'].isnull()]

    null_rows.reset_index(drop=True, inplace=True)

    check_null_values(null_rows)

    for index, row in null_rows.iterrows():
        title = row['title'].replace(' ', '+') # Si tiene espacios, hay que reemplazar esos espacios por un '+'

        if row['type'] == 'Movie':
            response = get_movie_details_by_title(title)
        else:
            response = get_tvshow_details_by_title(title)

        if response.status_code == 200:
            data = response.json()
            if data['total_results'] > 0:
                title_id = data['results'][0]['id']

                if row['type'] == 'Movie':
                    response = get_movie_details_by_id(title_id)
                else: 
                    response = get_tvshow_details_by_id(title_id)

                if response.status_code == 200:
                    data = response.json()
                    
                    genres = [item['name'] for item in data['genres']]
                    genres_string = ', '.join(genres)
                    index_df = df[df['show_id'] == row['show_id']].index[0]
                    df.at[index_df, 'listed_in'] = genres_string

    return df

def fill_release_year(df):
    # Obtener las filas con valores NaN en la columna 'release_year'
    null_rows = df[df['release_year'].isnull()]

    null_rows.reset_index(drop=True, inplace=True)

    check_null_values(null_rows)

    for index, row in null_rows.iterrows():
        title = row['title'].replace(' ', '+') # Si tiene espacios, hay que reemplazar esos espacios por un '+'

        if row['type'] == 'Movie':
            response = get_movie_details_by_title(title)
        else:
            response = get_tvshow_details_by_title(title)

        if response.status_code == 200:
            data = response.json()
            if data['total_results'] > 0:
                title_id = data['results'][0]['id']

                if row['type'] == 'Movie':
                    response = get_movie_details_by_id(title_id)
                else: 
                    response = get_tvshow_details_by_id(title_id)

                if response.status_code == 200:
                    data = response.json()

                    if row['type'] == 'Movie':
                        release_year = data['release_date']
                    else:
                        release_year = data['first_air_date'].split('-')[0]
                        
                    index_df = df[df['show_id'] == row['show_id']].index[0]
                    df.at[index_df, 'release_year'] = release_year

    return df

def fill_description(df):
    # Obtener las filas con valores NaN en la columna 'description'
    null_rows = df[df['description'].isnull()]

    null_rows.reset_index(drop=True, inplace=True)

    check_null_values(null_rows)

    for index, row in null_rows.iterrows():
        title = row['title'].replace(' ', '+') # Si tiene espacios, hay que reemplazar esos espacios por un '+'

        if row['type'] == 'Movie':
            response = get_movie_details_by_title(title)
        else:
            response = get_tvshow_details_by_title(title)

        if response.status_code == 200:
            data = response.json()
            if data['total_results'] > 0:
                title_id = data['results'][0]['id']

                if row['type'] == 'Movie':
                    response = get_movie_details_by_id(title_id)
                else: 
                    response = get_tvshow_details_by_id(title_id)

                if response.status_code == 200:
                    data = response.json()

                    # Overview/description
                    index_df = df[df['show_id'] == row['show_id']].index[0]
                    df.at[index_df, 'description'] = data['overview']

    return df

def fill_data(df):
    df = fill_cast(df)
    df = fill_director(df)
    df = fill_country(df)
    df = fill_genres(df)
    df = fill_release_year(df)
    df = fill_description(df)
    return df


# Cargar el DataFrame con los datos originales
# df = pd.read_csv('./output/merged_titles.csv')