import os
import asyncpg
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
from fastapi import FastAPI, HTTPException

app = FastAPI()

# Parameters for connection
params = {
    'host': os.environ.get('PGHOST'),
    'user': os.environ.get('PGUSER'),
    'password': os.environ.get('PGPASSWORD'),
    'port': os.environ.get('PGPORT'),
}

@app.get("/")
async def root():
    return {"message": "Hello World"}

@app.on_event("startup")
async def startup():
    print(params)
    app.state.pool = await asyncpg.create_pool(**params)

@app.on_event("shutdown")
async def shutdown():
    await app.state.pool.close()

@app.get("/articles")
async def get_articles():
    """
    Retrieve all articles from the database.

    Returns:
        dict: A dictionary containing the articles retrieved from the database.
              Each article is represented as a dictionary.
        dict: A dictionary containing an error message if an exception occurs.
    """
    try:
        async with app.state.pool.acquire() as connection:
            rows = await connection.fetch('SELECT * FROM articles')
            return {"articles": [dict(row) for row in rows]}
    except Exception as e:
        return {"error": str(e)}

@app.get("/articles/{article_id}")
async def get_articles_by_id(article_id: int):
    """
    Retrieve articles from the database by their ID.

    Args:
        article_id (int): The ID of the article to retrieve.

    Returns:
        dict: A dictionary containing the retrieved articles.

    Raises:
        Exception: If there is an error while retrieving the articles.
    """
    try:
        async with app.state.pool.acquire() as connection:
            rows = await connection.fetch('SELECT * FROM articles where article_id = $1', article_id)
            return {"articles": [dict(row) for row in rows]}
    except Exception as e:
        return {"error": str(e)}

@app.get("/articles/{article_id}/relevant")
async def set_relevant_article(article_id: int):
    """
    Sets the 'relevant' flag of an article to True.

    Parameters:
    - article_id (int): The ID of the article to update.

    Returns:
    - dict: A dictionary with the key "ok" if the update is successful, or a dictionary with the key "error" if an exception occurs.
    """
    try:
        async with app.state.pool.acquire() as connection:
            await connection.execute('UPDATE articles SET relevant = TRUE WHERE article_id = $1', article_id)
            row = await connection.fetchrow('SELECT * FROM articles WHERE article_id = $1', article_id)
            if row is None:
                raise HTTPException(status_code=404, detail="Article not found")
            return {"ok"}
    except Exception as e:
        return {"error": str(e)}

@app.get("/articles/{article_id}/irrelevant")
async def set_irrelevant_article(article_id: int):
    """
    Marks an article as irrelevant by updating its 'relevant' field to False in the database.

    Args:
        article_id (int): The ID of the article to mark as irrelevant.

    Returns:
        dict: A dictionary with the key "ok" if the operation was successful, or a dictionary with the key "error" if an exception occurred.
    """
    try:
        async with app.state.pool.acquire() as connection:
            await connection.execute('UPDATE articles SET relevant = FALSE WHERE article_id = $1', article_id)
            row = await connection.fetchrow('SELECT * FROM articles WHERE article_id = $1', article_id)
            if row is None:
                raise HTTPException(status_code=404, detail="Article not found")
            return {"ok"}
    except Exception as e:
        return {"error": str(e)}

@app.get("/incidents")
async def get_incidents():
    """
    Retrieve all incidents from the database.

    Returns:
        dict: A dictionary containing a list of incidents retrieved from the database.
              Each incident is represented as a dictionary.
        dict: A dictionary containing an error message if an exception occurs during the retrieval.
    """
    try:
        async with app.state.pool.acquire() as connection:
            rows = await connection.fetch('SELECT * FROM incidents')
            return {"incidents": [dict(row) for row in rows]}
    except Exception as e:
        return {"error": str(e)}
    
@app.get("/incidents/{incident_id}")
async def get_incident_by_id(incident_id: int):
    """
    Retrieve an incident by its ID.

    Parameters:
    - incident_id (int): The ID of the incident to retrieve.

    Returns:
    - dict: A dictionary containing the incident details and associated articles, if any.
    - If the incident is found, the dictionary will have the following structure:
        {
            "incident": [dict(row) for row in rows],
            "articles": [dict(row) for row in articles]
        }
    - If the incident is not found or an error occurs, the dictionary will have the following structure:
        {
            "error": str(e)
        }
    """
    try:
        async with app.state.pool.acquire() as connection:
            rows = await connection.fetch('SELECT * FROM incidents where incident_id = $1', incident_id)
            articles = await connection.fetch('SELECT article_id, website, title, date FROM articles WHERE article_id IN (SELECT article_id FROM mapping WHERE incident_id = $1)', incident_id)
            return {"incident": [dict(row) for row in rows], "articles": [dict(row) for row in articles]}
    except Exception as e:
        return {"error": str(e)}

@app.get("/incidents/{incident_id}/verified")
async def set_verified_incident(incident_id: int):
    """
    Sets the 'verified' status of an incident to True.

    Parameters:
    - incident_id (int): The ID of the incident to be verified.

    Returns:
    - dict: A dictionary with the key "ok" if the incident was successfully verified,
            or a dictionary with the key "error" and the error message if an exception occurred.
    """
    try:
        async with app.state.pool.acquire() as connection:
            await connection.execute('UPDATE incidents SET verified = TRUE WHERE incident_id = $1', incident_id)
            row = await connection.fetchrow('SELECT * FROM incidents WHERE incident_id = $1', incident_id)
            if row is None:
                raise HTTPException(status_code=404, detail="Incident not found")
            return {"ok"}
    except Exception as e:
        return {"error": str(e)}
    
@app.get("/incidents/{incident_id}/unverified")
async def set_verified_incident(incident_id: int):
    """
    Sets the 'verified' status of an incident to False.

    Parameters:
    - incident_id (int): The ID of the incident to update.
    
    Returns:
    - dict: A dictionary with the key "ok" if the update was successful, or a dictionary with the key "error" if an exception occurred.
    """
    try:
        async with app.state.pool.acquire() as connection:
            await connection.execute('UPDATE incidents SET verified = FALSE WHERE incident_id = $1', incident_id)
            row = await connection.fetchrow('SELECT * FROM incidents WHERE incident_id = $1', incident_id)
            if row is None:
                raise HTTPException(status_code=404, detail="Incident not found")
            return {"ok"}
    except Exception as e:
        return {"error": str(e)}
