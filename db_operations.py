import asyncpg
import os
import psycopg2
from psycopg2.extras import DictCursor

"""
This file contains functions to interact with the PostgreSQL database for debugging and administration.
"""

params = {
    "host": os.environ.get("PGHOST"),
    "user": os.environ.get("PGUSER"),
    "password": os.environ.get("PGPASSWORD"),
    "port": os.environ.get("PGPORT"),
    "dbname": os.environ.get("PGDATABASE"),
}


def pool():
    return asyncpg.create_pool(**params)


def delete_table(table_name):
    """
    Delete a table from the database.
    Args:
        table_name (str): The name of the table to be deleted.
    Returns:
        None
    Raises:
        Exception: If an error occurs while deleting the table.
    """
    conn = None
    try:
        conn = psycopg2.connect(**params)
        cursor = conn.cursor()
        cursor.execute(f"DROP TABLE {table_name}")
        conn.commit()
        print("Table deleted successfully")
    except Exception as e:
        print("An error occurred:", e)
    finally:
        if conn:
            cursor.close()
            conn.close()


def create_mapping_table():
    """
    Creates a mapping table in the database that maps incident IDs to article IDs.

    This function executes an SQL query to create a table named 'mapping' with two columns:
    - incident_id: INTEGER, a foreign key referencing the 'incident_id' column in the 'incidents' table.
    - article_id: INTEGER, a foreign key referencing the 'article_id' column in the 'articles' table.
    The primary key of the table is a combination of incident_id and article_id.

    Raises:
        Exception: If an error occurs while executing the SQL query.

    Returns:
        None
    """
    create_mapping_table_sql = """
    CREATE TABLE mapping (
    incident_id INTEGER REFERENCES incidents(incident_id),
    article_id INTEGER REFERENCES articles(article_id),
    PRIMARY KEY (incident_id, article_id)
);
    """
    conn = None
    # Connect to the PostgreSQL database
    try:
        conn = psycopg2.connect(**params)
        cursor = conn.cursor()
        cursor.execute(create_mapping_table_sql)
        conn.commit()
        print("Mapping Table created successfully")
    except Exception as e:
        print("An error occurred:", e)
    finally:
        if conn:
            cursor.close()
            conn.close()


def create_articles_table():
    """
    Creates the 'articles' table in the PostgreSQL database.

    The 'articles' table stores information about articles, including their title, summary, website URL,
    content, keywords, date, number of dead, number of missing, number of survivors, country of origin,
    region of origin, cause of death, region of incident, country of incident, location of incident,
    latitude, longitude, and relevance.

    Returns:
        None
    """
    create_articles_table_sql = """
    CREATE TABLE articles (
    article_id SERIAL PRIMARY KEY,
    title VARCHAR(255),
    summary TEXT,
    website VARCHAR(2048),  -- URL of the website
    content TEXT,           -- Content of the website URL
    keywords TEXT[],        -- List of keywords, using PostgreSQL array type
    date DATE,
    number_dead INTEGER,
    number_missing INTEGER,
    number_survivors INTEGER,
    country_of_origin VARCHAR(255),
    region_of_origin VARCHAR(255),
    cause_of_death TEXT, 
    region_of_incident VARCHAR(255),
    country_of_incident VARCHAR(255),
    location_of_incident TEXT,
    latitude DECIMAL(9,6),
    longitude DECIMAL(9,6),
    relevant BOOLEAN DEFAULT TRUE
);
    """
    conn = None
    # Connect to the PostgreSQL database
    try:
        conn = psycopg2.connect(**params)
        cursor = conn.cursor()
        cursor.execute(create_articles_table_sql)
        conn.commit()
        print("Articles Table created successfully")
    except Exception as e:
        print("An error occurred:", e)
    finally:
        if conn:
            cursor.close()
            conn.close()


def create_incidents_table():
    """
    Creates the 'incidents' table in the PostgreSQL database.

    This function executes an SQL command to create a table named 'incidents' with the following columns:
    - incident_id (SERIAL PRIMARY KEY)
    - title (VARCHAR(255))
    - verified (BOOLEAN)
    - date (DATE)
    - number_dead (INTEGER)
    - number_missing (INTEGER)
    - number_survivors (INTEGER)
    - country_of_origin (VARCHAR(255))
    - region_of_origin (VARCHAR(255))
    - cause_of_death (TEXT)
    - region_of_incident (VARCHAR(255))
    - country_of_incident (VARCHAR(255))
    - location_of_incident (TEXT)
    - latitude (DECIMAL(9,6))
    - longitude (DECIMAL(9,6))

    Parameters:
    None

    Returns:
    None
    """
    create_table_sql = """
    CREATE TABLE incidents (
    incident_id SERIAL PRIMARY KEY,
    title VARCHAR(255),
    verified BOOLEAN,
    date DATE,
    number_dead INTEGER,
    number_missing INTEGER,
    number_survivors INTEGER,
    country_of_origin VARCHAR(255),
    region_of_origin VARCHAR(255),
    cause_of_death TEXT, 
    region_of_incident VARCHAR(255),
    country_of_incident VARCHAR(255),
    location_of_incident TEXT,
    latitude DECIMAL(9,6),
    longitude DECIMAL(9,6)
);
    """
    conn = None
    # Connect to the PostgreSQL database
    try:
        # Connect to the database
        conn = psycopg2.connect(**params)
        cursor = conn.cursor()
        # Execute the SQL commands
        cursor.execute(create_table_sql)
        # Commit the changes in the database
        conn.commit()
        print("Incidents Table created successfully")
    except Exception as e:
        print("An error occurred:", e)
    finally:
        # Closing the connection
        if conn:
            cursor.close()
            conn.close()


def insert_article(article):
    """
    Insert an article into the 'articles' table in the database.

    Args:
        article (dict): A dictionary containing the article data.

    Raises:
        Exception: If an error occurs while inserting the record.

    Returns:
        None
    """
    try:
        conn = psycopg2.connect(**params)
        cursor = conn.cursor()
        cursor.execute(
            """
            INSERT INTO articles (title, summary, website, content, keywords, date, number_dead, number_missing, number_survivors, country_of_origin, region_of_origin, cause_of_death, region_of_incident, country_of_incident, location_of_incident, latitude, longitude) 
            VALUES (%s,%s,%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """,
            (
                article["title"],
                article["summary"],
                article["website"],
                article["content"],
                article["keywords"],
                article["date"],
                article["number_dead"],
                article["number_missing"],
                article["number_survivors"],
                article["country_of_origin"],
                article["region_of_origin"],
                article["cause_of_death"],
                article["region_of_incident"],
                article["country_of_incident"],
                article["location_of_incident"],
                article["latitude"],
                article["longitude"],
            ),
        )
        conn.commit()
        print("Record inserted successfully")
    except Exception as e:
        print("An error occurred:", e)
    finally:
        if conn:
            cursor.close()
            conn.close()


def insert_incident(incident):
    """
    Inserts an incident record into the database.

    Args:
        incident (dict): A dictionary containing the details of the incident.
            The dictionary should have the following keys:
            - 'title' (str): The title of the incident.
            - 'verified' (bool): Indicates whether the incident is verified or not.
            - 'date' (str): The date of the incident.
            - 'number_dead' (int): The number of people dead in the incident.
            - 'number_missing' (int): The number of people missing in the incident.
            - 'number_survivors' (int): The number of survivors in the incident.
            - 'country_of_origin' (str): The country of origin of the incident.
            - 'region_of_origin' (str): The region of origin of the incident.
            - 'cause_of_death' (str): The cause of death in the incident.
            - 'region_of_incident' (str): The region of the incident.
            - 'country_of_incident' (str): The country of the incident.
            - 'location_of_incident' (str): The location of the incident.
            - 'latitude' (float): The latitude of the incident location.
            - 'longitude' (float): The longitude of the incident location.

    Returns:
        int: The incident ID of the inserted record.

    Raises:
        Exception: If an error occurs during the insertion process.
    """
    try:
        conn = psycopg2.connect(**params)
        cursor = conn.cursor()
        cursor.execute(
            """
            INSERT INTO incidents (title,verified, date, number_dead, number_missing, number_survivors, country_of_origin, region_of_origin, cause_of_death, region_of_incident, country_of_incident, location_of_incident, latitude, longitude) 
            VALUES (%s,%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            RETURNING incident_id
            """,
            (
                incident["title"],
                incident["verified"],
                incident["date"],
                incident["number_dead"],
                incident["number_missing"],
                incident["number_survivors"],
                incident["country_of_origin"],
                incident["region_of_origin"],
                incident["cause_of_death"],
                incident["region_of_incident"],
                incident["country_of_incident"],
                incident["location_of_incident"],
                incident["latitude"],
                incident["longitude"],
            ),
        )
        incident_id = cursor.fetchone()[0]
        conn.commit()
        print("Record inserted successfully")
        return incident_id
    except Exception as e:
        print("An error occurred:", e)
    finally:
        if conn:
            cursor.close()
            conn.close()


def insert_mapping(incident_id, article_id):
    """
    Inserts a mapping record into the 'mapping' table in the database.

    Parameters:
    - incident_id (int): The ID of the incident.
    - article_id (int): The ID of the article.

    Returns:
    None
    """
    try:
        conn = psycopg2.connect(**params)
        cursor = conn.cursor()
        cursor.execute(
            """
            INSERT INTO mapping (incident_id, article_id) 
            VALUES (%s, %s)
            """,
            (incident_id, article_id),
        )
        conn.commit()
        print("Record inserted successfully")
    except Exception as e:
        print("An error occurred:", e)
    finally:
        if conn:
            cursor.close()
            conn.close()


def get_all_incidents():
    """
    Retrieve all incidents from the database.

    Returns:
        list: A list of tuples representing the incidents.
    """
    try:
        conn = psycopg2.connect(**params)
        cursor = conn.cursor()
        cursor.execute("SELECT * FROM Incidents")
        incidents = cursor.fetchall()
        return incidents
    except Exception as e:
        print("An error occurred:", e)
    finally:
        if conn:
            cursor.close()
            conn.close()


def get_all_articles():
    """
    Retrieve all articles from the database.

    Returns:
        list: A list of tuples representing the articles.
    """
    try:
        conn = psycopg2.connect(**params)
        cursor = conn.cursor()
        cursor.execute("SELECT * FROM articles")
        articles = cursor.fetchall()
        return articles
    except Exception as e:
        print("An error occurred:", e)
    finally:
        if conn:
            cursor.close()
            conn.close()


# dummy data for a single incident
incident = {
    "title": "Migrant boat capsizes in Mediterranean",
    "verified": True,
    "date": "2021-05-06",
    "number_dead": 20,
    "number_missing": 10,
    "number_survivors": 30,
    "country_of_origin": "Libya",
    "region_of_origin": "North Africa",
    "cause_of_death": "Drowning",
    "region_of_incident": "Mediterranean",
    "country_of_incident": "Italy",
    "location_of_incident": "Mediterranean Sea",
    "latitude": 41.9028,
    "longitude": 12.4964,
}

# dummy data for a sigle article
article = {
    "title": "Migrant boat capsizes in Mediterranean",
    "summary": "20 migrants drown after their boat capsizes in the Mediterranean Sea",
    "website": "https://www.bbc.com/news/world-africa-57033012",
    "content": "The content of the article",
    "keywords": ["migrant", "drown", "boat"],
    "date": "2021-05-06",
    "number_dead": 20,
    "number_missing": 10,
    "number_survivors": 30,
    "country_of_origin": "Libya",
    "region_of_origin": "North Africa",
    "cause_of_death": "Drowning",
    "region_of_incident": "Mediterranean",
    "country_of_incident": "Italy",
    "location_of_incident": "Mediterranean Sea",
    "latitude": 41.9028,
    "longitude": 12.4964,
}

mapping = {"incident_id": 1, "article_id": 1}


def days_between(d1, d2):
    """
    Calculate the number of days between two dates.

    Parameters:
    d1 (datetime.date): The first date.
    d2 (datetime.date): The second date.

    Returns:
    int: The number of days between the two dates.
    """
    return abs((d2 - d1).days)


def is_close_location(
    loc1, loc2, threshold=50
):  # Dummy function, replace with real geospatial logic
    pass  # future work: implement this function to check if two locations are close


# Function to group similar articles
def are_similar_articles(article1, article2):
    date_similarity = days_between(article1[1], article2[1]) <= 3
    return date_similarity

    # future work:
    # location_similarity = is_close_location(article1[3], article2[3])
    # return date_similarity and location_similarity


def group_articles(articles):
    """
    Groups similar articles together based on a similarity criterion.

    Args:
        articles (list): A list of articles to be grouped.

    Returns:
        list: A list of groups, where each group is a list of article IDs.
    """
    groups = []
    used = set()
    for i, article1 in enumerate(articles):
        if i in used:
            continue
        current_group = [article1[0]]
        for j, article2 in enumerate(articles[i + 1 :], start=i + 1):
            if j in used:
                continue
            if are_similar_articles(article1, article2):
                current_group.append(article2[0])
                used.add(j)
        groups.append(current_group)
        used.add(i)
    print(groups)
    return groups


def get_articles():
    """
    Retrieve all articles but only some attributes relevant for the grouping.

    Returns:
        list: A list of tuples representing the articles. Each tuple contains the following information:
            - article_id: The ID of the article.
            - date: The date of the article.
            - cause_of_death: The cause of death mentioned in the article.
            - location_of_incident: The location of the incident mentioned in the article.
            - country_of_incident: The country of the incident mentioned in the article.
            - region_of_incident: The region of the incident mentioned in the article.
    """
    try:
        conn = psycopg2.connect(**params)
        cursor = conn.cursor()
        cursor.execute(
            "SELECT article_id, date, cause_of_death, location_of_incident, country_of_incident, region_of_incident FROM articles"
        )
        articles = cursor.fetchall()
        print(articles)
        return articles
    except Exception as e:
        print("An error occurred:", e)
    finally:
        if conn:
            cursor.close()
            conn.close()


def update_incident_mappings(grouped_articles, articles):
    """
    Updates the incident mappings in the database based on the provided grouped articles.

    Args:
        grouped_articles (list): A list of lists, where each inner list represents a group of article IDs.
        articles (list): A list of article IDs.

    Returns:
        None
    """
    conn = psycopg2.connect(**params)
    cursor = conn.cursor(cursor_factory=DictCursor)
    try:
        for group in grouped_articles:
            cursor.execute(
                "SELECT * FROM articles where article_id = %s", (str(group[0]),)
            )
            first = cursor.fetchone()
            first_dict = dict(first)
            incident = {
                "title": first_dict["title"],
                "verified": True,
                "date": first_dict["date"],
                "number_dead": first_dict["number_dead"],
                "number_missing": first_dict["number_missing"],
                "number_survivors": first_dict["number_survivors"],
                "country_of_origin": first_dict["country_of_origin"],
                "region_of_origin": first_dict["region_of_origin"],
                "cause_of_death": first_dict["cause_of_death"],
                "region_of_incident": first_dict["region_of_incident"],
                "country_of_incident": first_dict["country_of_incident"],
                "location_of_incident": first_dict["location_of_incident"],
                "latitude": first_dict["latitude"],
                "longitude": first_dict["longitude"],
            }
            incident_id = insert_incident(incident)
            print("New incident ID:", incident_id)
            for article_id in group:
                cursor.execute(
                    "INSERT INTO mapping (article_id, incident_id) VALUES (%s, %s)",
                    (article_id, incident_id),
                )
        conn.commit()
    except Exception as e:
        print("Database update failed:", e)
        conn.rollback()
    finally:
        cursor.close()
        conn.close()


def process_articles():
    """
    Process articles by retrieving them, grouping them, and updating incident mappings.
    For now this function deletes all mappings before creating new ones.

    This function performs the following steps:
    1. Retrieves articles using the `get_articles` function.
    2. Groups the articles using the `group_articles` function.
    3. Updates incident mappings using the `update_incident_mappings` function.

    This function does not return any value.
    """
    delete_table("mapping")
    create_mapping_table()
    articles = get_articles()
    grouped_articles = group_articles(articles)
    update_incident_mappings(grouped_articles, articles)


# Call this function to process the articles
# process_articles()


def delete_entries():
    """
    To reset the database, this function deletes entries from the 'mapping', 'articles', and 'incidents' tables.
    Then recreates the 'articles', 'incidents', and 'mapping' tables.
    """
    delete_table("mapping")
    delete_table("articles")
    delete_table("incidents")
    create_articles_table()
    create_incidents_table()
    create_mapping_table()
