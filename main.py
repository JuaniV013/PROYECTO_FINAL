from fastapi import FastAPI
import pandas as pd
import mysql.connector
from collections import Counter


app = FastAPI()

df = pd.read_csv("data.csv")



@app.get("/max_duration")
async def get_max_duration(year: int = None, platform: str = None, duration_type: str = None):
    filtered_df = df.copy()
    if platform == 'netflix':
        if year is not None:
            filtered_df = filtered_df[filtered_df['release_year'] == year]
        if platform is not None:
            filtered_df = filtered_df[filtered_df['ID'].str.contains('n')]
        if duration_type is not None:
            filtered_df = filtered_df[filtered_df['duration_type'] == duration_type]

        max_duration = filtered_df['duration_int'].max()
        movie = filtered_df[filtered_df['duration_int'] == max_duration].iloc[0]
    elif platform == 'amazon':
        if year is not None:
            filtered_df = filtered_df[filtered_df['release_year'] == year]
        if platform is not None:
            filtered_df = filtered_df[filtered_df['ID'].str.contains('a')]
        if duration_type is not None:
            filtered_df = filtered_df[filtered_df['duration_type'] == duration_type]

        max_duration = filtered_df['duration_int'].max()
        movie = filtered_df[filtered_df['duration_int'] == max_duration].iloc[0]
    elif platform == 'disney':
        if year is not None:
            filtered_df = filtered_df[filtered_df['release_year'] == year]
        if platform is not None:
            filtered_df = filtered_df[filtered_df['ID'].str.contains('d')]
        if duration_type is not None:
            filtered_df = filtered_df[filtered_df['duration_type'] == duration_type]

        max_duration = filtered_df['duration_int'].max()
        movie = filtered_df[filtered_df['duration_int'] == max_duration].iloc[0]
    elif platform == 'hulu':
        if year is not None:
            filtered_df = filtered_df[filtered_df['release_year'] == year]
        if platform is not None:
            filtered_df = filtered_df[filtered_df['ID'].str.contains('h')]
        if duration_type is not None:
            filtered_df = filtered_df[filtered_df['duration_type'] == duration_type]

    
        max_duration = filtered_df['duration_int'].max()
        movie = filtered_df[filtered_df['duration_int'] == max_duration].iloc[0]

    return movie['title']


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
    if platform == 'netflix':
        platform_filter = df['ID'].str.contains('n')
    elif platform == 'disney':
        platform_filter = df['ID'].str.contains('d')
    elif platform == 'amazon':
        platform_filter = df['ID'].str.contains('a')
    elif platform == 'hulu':
        platform_filter = df['ID'].str.contains('h')
    
    result = df[platform_filter]['ID'].astype(str)
    result = result.value_counts()

    response = [{"plataforma": platform, "cantidad": result}]


    return response


@app.get("/Actor")
def get_actor(platform: str, year: int):

    if platform == 'netflix':
        platform_mask = df['ID'].str.contains('n')
    elif platform == 'disney':
        platform_mask = df['ID'].str.contains('d')
    elif platform == 'amazon':
        platform_mask = df['ID'].str.contains('a')
    elif platform == 'hulu':
        platform_mask = df['ID'].str.contains('h')

    year_mask = df['release_year'] == year
    mask = platform_mask & year_mask

    result = df.loc[mask, 'cast']
    result = result.dropna()
    
    actores = []
    for r in result:
        reparto = r.split(",")
        actores.extend(reparto)
    while '' in actores:
        actores.remove('')

    conteo = Counter(actores)

    actor_mas_repetido = conteo.most_common()
    actor_mas_repetido_final = actor_mas_repetido[0][0]

    return actor_mas_repetido_final
