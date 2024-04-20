from fastapi import FastAPI, HTTPException
from dotenv import load_dotenv
import os
import psycopg2
import asyncpg
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
from pydantic import BaseModel

app = FastAPI()

# Parameters for connection
load_dotenv()
params = {
    'host': os.getenv('PGHOST'),
    'user': os.getenv('PGUSER'),
    'password': os.getenv('PGPASSWORD'),
    'port': os.getenv('PGPORT'),
}

# Create a connection pool
async def create_pool():
    return await asyncpg.create_pool(**params)

pool = app.state.pool = create_pool()

@app.get("/")
async def root():
    return {"message": "Hello World"}

@app.on_event("startup")
async def startup():
    app.state.pool = await create_pool()

@app.on_event("shutdown")
async def shutdown():
    await app.state.pool.close()

@app.get("/articles")
async def get_articles():
    try:
        async with app.state.pool.acquire() as connection:
            rows = await connection.fetch('SELECT * FROM articles')
            return {"articles": [dict(row) for row in rows]}
    except Exception as e:
        return {"error": str(e)}

# get article by id
@app.get("/articles/{article_id}")
async def get_article(article_id: int):
    try:
        async with app.state.pool.acquire() as connection:
            row = await connection.fetchrow('SELECT * FROM articles WHERE article_id = $1', article_id)
            if row:
                return {"article": dict(row)}
            else:
                raise HTTPException(status_code=404, detail="Article not found")
    except asyncpg.exceptions.PostgresError as e:
        return {"error": str(e)}
    

@app.get("/incidents")
async def get_articles():
    try:
        async with app.state.pool.acquire() as connection:
            rows = await connection.fetch('SELECT * FROM incidents')
            return {"incidents": [dict(row) for row in rows]}
    except Exception as e:
        return {"error": str(e)}
    
# get incidents by article id
@app.get("/incidents/{article_id}")
async def get_article(article_id: int):
    try:
        async with app.state.pool.acquire() as connection:
            row = await connection.fetchrow('SELECT * FROM incidents WHERE incident_id = $1', article_id)
            if row:
                return {"incident": dict(row)}
            else:
                raise HTTPException(status_code=404, detail="Incident not found")
    except asyncpg.exceptions.PostgresError as e:
        return {"error": str(e)}

dummy_article = {"article":{"article_id":1,"website":"https://www.bbc.com/news/world-africa-57033012","content":"The content of the article","keywords":["migrant","drown","boat"],"date":"2021-05-06","number_dead":20,"number_missing":10,"number_survivors":30,"country_of_origin":"Libya","region_of_origin":"North Africa","cause_of_death":"Drowning","region_of_incident":"Mediterranean","country_of_incident":"Italy","location_of_incident":"Mediterranean Sea","latitude":41.9028,"longitude":12.4964}}


# write a function to post an article and insert it into the database
@app.post("/articles")
async def post_article(article: dict):
    try:
        async with app.state.pool.acquire() as connection:
            await connection.execute('INSERT INTO articles (website, content, keywords, date, number_dead, number_missing, number_survivors, country_of_origin, region_of_origin, cause_of_death, region_of_incident, country_of_incident, location_of_incident, latitude, longitude) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15)', article['website'], article['content'], article['keywords'], article['date'], article['number_dead'], article['number_missing'], article['number_survivors'], article['country_of_origin'], article['region_of_origin'], article['cause_of_death'], article['region_of_incident'], article['country_of_incident'], article['location_of_incident'], article['latitude'], article['longitude'])
            return {"message": "Article added successfully"}
    except asyncpg.exceptions.PostgresError as e:
        return {"error": str(e)}
    

class Article(BaseModel):
    website: str
    content: str
    keywords: list[str]
    date: str
    number_dead: int
    number_missing: int
    number_survivors: int
    country_of_origin: str
    region_of_origin: str
    cause_of_death: str
    region_of_incident: str
    country_of_incident: str
    location_of_incident: str
    latitude: float
    longitude: float

# app.state.pool = await asyncpg.create_pool(...)

article = {
    'website': 'https://www.bbc.com/news/world-africa-57033012',
    'content': 'The content of the article',
    'keywords': ['migrant', 'drown', 'boat'],
    'date': '2021-05-06',
    'number_dead': 20,
    'number_missing': 10,
    'number_survivors': 30,
    'country_of_origin': 'Libya',
    'region_of_origin': 'North Africa',
    'cause_of_death': 'Drowning',
    'region_of_incident': 'Mediterranean',
    'country_of_incident': 'Italy',
    'location_of_incident': 'Mediterranean Sea',
    'latitude': 41.9028,
    'longitude': 12.4964
}

@app.post("/articles")
async def post_article(article: dict):
    try:
        async with app.state.pool.acquire() as connection:
            await connection.execute(
                'INSERT INTO articles (website, content, keywords, date, number_dead, number_missing, number_survivors, country_of_origin, region_of_origin, cause_of_death, region_of_incident, country_of_incident, location_of_incident, latitude, longitude) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15)',
                article.website, article.content, ', '.join(article.keywords), article.date, article.number_dead, article.number_missing, article.number_survivors, article.country_of_origin, article.region_of_origin, article.cause_of_death, article.region_of_incident, article.country_of_incident, article.location_of_incident, article.latitude, article.longitude
            )
            return {"message": "Article added successfully"}
    except asyncpg.exceptions.PostgresError as e:
        # Handling database errors
        raise HTTPException(status_code=500, detail="Database error: " + str(e))
    except KeyError as e:
        # Handling missing keys in the article dictionary
        raise HTTPException(status_code=400, detail="Missing key: " + str(e))

x=True
while x:
    print("Enter the article data:")
    post_article(article)
    x = False
    