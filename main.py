from dotenv import load_dotenv
import os
import psycopg2
import asyncpg
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
from fastapi import FastAPI, HTTPException

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
    
@app.get("/articles/{article_id}")
async def get_articles_by_id(article_id: int):
    try:
        async with app.state.pool.acquire() as connection:
            rows = await connection.fetch('SELECT * FROM articles where article_id = $1', article_id)
            return {"articles": [dict(row) for row in rows]}
    except Exception as e:
        return {"error": str(e)}
    
@app.get("/articles/{article_id}/relevant")
async def set_relvant_article(article_id: int):
    try:
        async with app.state.pool.acquire() as connection:
            # Update the verified column to True for the incident with the given ID
            await connection.execute('UPDATE articles SET verified = FALSE WHERE article_id = $1', article_id)
            
            # Fetch the updated incident
            row = await connection.fetchrow('SELECT * FROM articles WHERE article_id = $1', article_id)
            if row is None:
                raise HTTPException(status_code=404, detail="Article not found")
            
            # Return the updated incident
            return {"article": dict(row)}
    except Exception as e:
        return {"error": str(e)}
    

@app.get("/incidents")
async def get_incidents():
    try:
        async with app.state.pool.acquire() as connection:
            rows = await connection.fetch('SELECT * FROM incidents')
            return {"incidents": [dict(row) for row in rows]}
    except Exception as e:
        return {"error": str(e)}
    
@app.get("/incidents/{incident_id}")
async def get_incident_by_id(incident_id: int):
    try:
        async with app.state.pool.acquire() as connection:
            rows = await connection.fetch('SELECT * FROM incidents where incident_id = $1', incident_id)
            return {"incident": [dict(row) for row in rows]}
    except Exception as e:
        return {"error": str(e)}


@app.get("/incidents/{incident_id}/verified")
async def set_verified_incident(incident_id: int):
    try:
        async with app.state.pool.acquire() as connection:
            # Update the verified column to True for the incident with the given ID
            await connection.execute('UPDATE incidents SET verified = TRUE WHERE incident_id = $1', incident_id)
            
            # Fetch the updated incident
            row = await connection.fetchrow('SELECT * FROM incidents WHERE incident_id = $1', incident_id)
            if row is None:
                raise HTTPException(status_code=404, detail="Incident not found")
            
            # Return the updated incident
            return {"ok"}
    except Exception as e:
        return {"error": str(e)}
    
@app.get("/incidents/{incident_id}/unverified")
async def set_verified_incident(incident_id: int):
    try:
        async with app.state.pool.acquire() as connection:
            # Update the verified column to True for the incident with the given ID
            await connection.execute('UPDATE incidents SET verified = FALSE WHERE incident_id = $1', incident_id)
            
            # Fetch the updated incident
            row = await connection.fetchrow('SELECT * FROM incidents WHERE incident_id = $1', incident_id)
            if row is None:
                raise HTTPException(status_code=404, detail="Incident not found")
            
            # Return the updated incident
            return {"ok"}
    except Exception as e:
        return {"error": str(e)}