from fastapi import FastAPI
import pandas as pd
import mysql.connector
from collections import Counter
from deta import Deta

app = FastAPI()

# Cargar los datos a un dataframe
df = pd.read_csv("data.csv")
deta = Deta("e0cz2sltxpa_QU1946uT44Dvz8zmfzGtBiu8DHDAFi2y")
drive = deta.Drive("date")


# Definir la estructura del endpoint
@app.get("/max_duration")
async def get_max_duration(year: int = None, platform: str = None, duration_type: str = None):
    # Filtrar los datos según los parámetros recibidos
    filtered_df = df.copy()
    if platform == 'netflix':
        if year is not None:
            filtered_df = filtered_df[filtered_df['release_year'] == year]
        if platform is not None:
            filtered_df = filtered_df[filtered_df['ID'].str.contains('n')]
        if duration_type is not None:
            filtered_df = filtered_df[filtered_df['duration_type'] == duration_type]

        # Encontrar la película con la mayor duración
        max_duration = filtered_df['duration_int'].max()
        movie = filtered_df[filtered_df['duration_int'] == max_duration].iloc[0]
    elif platform == 'amazon':
        if year is not None:
            filtered_df = filtered_df[filtered_df['release_year'] == year]
        if platform is not None:
            filtered_df = filtered_df[filtered_df['ID'].str.contains('a')]
        if duration_type is not None:
            filtered_df = filtered_df[filtered_df['duration_type'] == duration_type]

        # Encontrar la película con la mayor duración
        max_duration = filtered_df['duration_int'].max()
        movie = filtered_df[filtered_df['duration_int'] == max_duration].iloc[0]
    elif platform == 'disney':
        if year is not None:
            filtered_df = filtered_df[filtered_df['release_year'] == year]
        if platform is not None:
            filtered_df = filtered_df[filtered_df['ID'].str.contains('d')]
        if duration_type is not None:
            filtered_df = filtered_df[filtered_df['duration_type'] == duration_type]

        # Encontrar la película con la mayor duración
        max_duration = filtered_df['duration_int'].max()
        movie = filtered_df[filtered_df['duration_int'] == max_duration].iloc[0]
    elif platform == 'hulu':
        if year is not None:
            filtered_df = filtered_df[filtered_df['release_year'] == year]
        if platform is not None:
            filtered_df = filtered_df[filtered_df['ID'].str.contains('h')]
        if duration_type is not None:
            filtered_df = filtered_df[filtered_df['duration_type'] == duration_type]

        # Encontrar la película con la mayor duración
        max_duration = filtered_df['duration_int'].max()
        movie = filtered_df[filtered_df['duration_int'] == max_duration].iloc[0]
    # Devolver los resultados en el formato especificado
    return movie['title']


# Endpoint to get score count by platform and year
@app.get("/score_count/")
async def get_score_count(platform: str, scored: float):
    cnx = mysql.connector.connect(user='root', password='12345678', host='127.0.0.1', database='proyecto_individual')
    cursor = cnx.cursor()

    if platform == 'netflix':
        query = f"SELECT movieId, AVG(rating) as scored FROM tabla1 WHERE movieId LIKE '%n%' AND scored >= {scored} GROUP BY movieID;"
        cursor.execute(query)
        results = cursor.fetchall()

        score_count = {}

        for row in results:
            platform = row[0]
            count = row[1]
            score_count.append({"platform": 'netflix', "count": count})

        cursor.close()
        cnx.close()

    return results


@app.get("/peliculas")
def get_count_platform(platform: str = None):
    cnx = mysql.connector.connect(user='root', password='12345678', host='127.0.0.1', database='proyecto_individual')
    cursor = cnx.cursor()

    if platform == 'netflix':
        query = f"SELECT ID, COUNT(*) FROM tabla_union_prueba WHERE ID LIKE '%n%';"
    elif platform == 'disney':
        query = f"SELECT ID, COUNT(*) FROM tabla_union_prueba WHERE ID LIKE '%d%';"
    elif platform == 'amazon':
        query = f"SELECT ID, COUNT(*) FROM tabla_union_prueba WHERE ID LIKE '%a%';"
    elif platform == 'hulu':
        query = f"SELECT ID, COUNT(*) FROM tabla_union_prueba WHERE ID LIKE '%h%';"
    else:
        query = "SELECT ID, COUNT(*) FROM tabla_union_prueba GROUP BY ID"

    cursor.execute(query)
    result = cursor.fetchall()

    response = [{"plataforma": platform, "cantidad": r[1]} for r in result]

    return response

@app.get("/actor_mas_repetido")
def get_actor(platform: str, year: int):
    cnx = mysql.connector.connect(user='root', password='12345678', host='127.0.0.1', database='proyecto_individual')
    cursor = cnx.cursor()

    if platform == 'netflix':
        query = f"SELECT cast FROM tabla_union_prueba WHERE ID LIKE '%n%' AND release_year = {year}"
        cursor.execute(query)
        result = cursor.fetchall()

        actores = []

        for r in result:
            reparto = r[0].split(",")
            actores.extend(reparto)

        while '' in actores:
            actores.remove('')

        conteo = Counter(actores)

        actor_mas_repetido = conteo.most_common()
        actor_mas_repetido_final = actor_mas_repetido[0][0]
    elif platform == 'disney':
        query = f"SELECT cast FROM tabla_union_prueba WHERE ID LIKE '%d%' AND release_year = {year}"
        cursor.execute(query)
        result = cursor.fetchall()

        actores = []

        for r in result:
            reparto = r[0].split(",")
            actores.extend(reparto)

        while '' in actores:
            actores.remove('')

        conteo = Counter(actores)

        actor_mas_repetido = conteo.most_common()
        actor_mas_repetido_final = actor_mas_repetido[0][0]
    elif platform == 'amazon':
        query = f"SELECT cast FROM tabla_union_prueba WHERE ID LIKE '%a%' AND release_year = {year}"
        cursor.execute(query)
        result = cursor.fetchall()

        actores = []

        for r in result:
            reparto = r[0].split(",")
            actores.extend(reparto)

        while '' in actores:
            actores.remove('')

        conteo = Counter(actores)

        actor_mas_repetido = conteo.most_common()
        actor_mas_repetido_final = actor_mas_repetido[0][0]
    elif platform == 'hulu':
        query = f"SELECT cast FROM tabla_union_prueba WHERE ID LIKE '%n%' AND release_year = {year}"
        cursor.execute(query)
        result = cursor.fetchall()

        actores = []

        for r in result:
            reparto = r[0].split(",")
            actores.extend(reparto)

        while '' in actores:
            actores.remove('')

        conteo = Counter(actores)

        actor_mas_repetido = conteo.most_common()
        actor_mas_repetido_final = actor_mas_repetido[0][0]
    return actor_mas_repetido_final
