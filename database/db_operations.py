from dotenv import load_dotenv
import os
import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
from psycopg2 import Error

# Load environment variables from .env file
load_dotenv()
# Parameters for connection
params = {
    'host': os.getenv('PGHOST'),
    'user': os.getenv('PGUSER'),
    'password': os.getenv('PGPASSWORD'),
    'port': os.getenv('PGPORT'),
    'dbname': os.getenv('PGDATABASE')
}

def delete_table(table_name):
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
    # SQL that maps icidentID to articleID
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
    longitude DECIMAL(9,6)
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
    try:
        conn = psycopg2.connect(**params)
        cursor = conn.cursor()
        cursor.execute(
            """
            INSERT INTO articles (title, summary, website, content, keywords, date, number_dead, number_missing, number_survivors, country_of_origin, region_of_origin, cause_of_death, region_of_incident, country_of_incident, location_of_incident, latitude, longitude) 
            VALUES (%s,%s,%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, 
            (article['title'], article['summary'], article['website'], article['content'], article['keywords'], article['date'], article['number_dead'], article['number_missing'], article['number_survivors'], article['country_of_origin'], article['region_of_origin'], article['cause_of_death'], article['region_of_incident'], article['country_of_incident'], article['location_of_incident'], article['latitude'], article['longitude'])
        )
        conn.commit()
        print("Record inserted successfully")
    except Exception as e:
        print("An error occurred:", e)
    finally:
        if conn:
            cursor.close()
            conn.close()

# write a function to insert an incident
def insert_incident(incident):
    try:
        conn = psycopg2.connect(**params)
        cursor = conn.cursor()
        cursor.execute(
             """
            INSERT INTO incidents (title,verified, date, number_dead, number_missing, number_survivors, country_of_origin, region_of_origin, cause_of_death, region_of_incident, country_of_incident, location_of_incident, latitude, longitude) 
            VALUES (%s,%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """,
            (incident['title'],incident['verified'], incident['date'], incident['number_dead'], incident['number_missing'], incident['number_survivors'], incident['country_of_origin'], incident['region_of_origin'], incident['cause_of_death'], incident['region_of_incident'], incident['country_of_incident'], incident['location_of_incident'], incident['latitude'], incident['longitude'])
        )
        conn.commit()
        print("Record inserted successfully")
    except Exception as e:
        print("An error occurred:", e)
    finally:
        if conn:
            cursor.close()
            conn.close()

# write dummy data for a single incident
incident = {
    'title': 'Migrant boat capsizes in Mediterranean',
    'verified': True,
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

# write a function to get all the incidents
def get_all_incidents():
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


#delete_table('mapping')
#delete_table('articles')
#delete_table('incidents')

#create_articles_table()
#create_incidents_table()
#create_mapping_table()


# write dummy data for a sigle article
article = {
    'title': 'Migrant boat capsizes in Mediterranean',
    'summary': '20 migrants drown after their boat capsizes in the Mediterranean Sea',
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

#insert_article(article)
#insert_incident(incident)