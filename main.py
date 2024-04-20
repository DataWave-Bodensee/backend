from fastapi import FastAPI
from dotenv import load_dotenv
import os
import psycopg2
import asyncpg
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT

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
    
