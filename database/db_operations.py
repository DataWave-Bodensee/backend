from dotenv import load_dotenv
import os
import psycopg2
from psycopg2.extras import DictCursor
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
from psycopg2 import Error
import datetime

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
            RETURNING incident_id
            """,
            (incident['title'],incident['verified'], incident['date'], incident['number_dead'], incident['number_missing'], incident['number_survivors'], incident['country_of_origin'], incident['region_of_origin'], incident['cause_of_death'], incident['region_of_incident'], incident['country_of_incident'], incident['location_of_incident'], incident['latitude'], incident['longitude'])
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
    try:
        conn = psycopg2.connect(**params)
        cursor = conn.cursor()
        cursor.execute(
            """
            INSERT INTO mapping (incident_id, article_id) 
            VALUES (%s, %s)
            """,
            (incident_id, article_id)
        )
        conn.commit()
        print("Record inserted successfully")
    except Exception as e:
        print("An error occurred:", e)
    finally:
        if conn:
            cursor.close()
            conn.close()

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

def get_all_articles():
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

mapping = {
    'incident_id': 1,
    'article_id': 1
}

#insert_mapping(1, 2)
#insert_article(article)
#insert_incident(incident)

def days_between(d1, d2):
    return abs((d2 - d1).days)

def is_close_location(loc1, loc2, threshold=50):  # Dummy function, replace with real geospatial logic
    pass

# Function to group similar articles
def are_similar_articles(article1, article2):
    date_similarity = days_between(article1[1], article2[1]) <= 10
    return date_similarity

    #future work: 
    #location_similarity = is_close_location(article1[3], article2[3])
    #return date_similarity and location_similarity

def group_articles(articles):
    groups = []
    used = set()
    for i, article1 in enumerate(articles):
        if i in used:
            continue
        current_group = [article1[0]]
        for j, article2 in enumerate(articles[i+1:], start=i+1):
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
    try:
        conn = psycopg2.connect(**params)
        cursor = conn.cursor()
        cursor.execute("SELECT article_id, date, cause_of_death, location_of_incident, country_of_incident, region_of_incident FROM articles")
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
    conn = psycopg2.connect(**params)
    cursor = conn.cursor(cursor_factory=DictCursor)
    try:
        for group in grouped_articles:
            cursor.execute('SELECT * FROM articles where article_id = %s',  (str(group[0]),))
            first = cursor.fetchone()
            first_dict = dict(first)
            incident = {
                'title': first_dict['title'],
                'verified': True,
                'date': first_dict['date'],
                'number_dead': first_dict['number_dead'],
                'number_missing':  first_dict['number_missing'],
                'number_survivors': first_dict['number_survivors'],
                'country_of_origin': first_dict['country_of_origin'],
                'region_of_origin': first_dict['region_of_origin'],
                'cause_of_death': first_dict['cause_of_death'],
                'region_of_incident': first_dict['region_of_incident'],
                'country_of_incident': first_dict['country_of_incident'],
                'location_of_incident': first_dict['location_of_incident'],
                'latitude':first_dict['latitude'],
                'longitude': first_dict['longitude']
            }
            incident_id = insert_incident(incident)
            print("New incident ID:", incident_id)
            for article_id in group:
                cursor.execute("INSERT INTO mapping (article_id, incident_id) VALUES (%s, %s)", (article_id, incident_id))
        conn.commit()
    except Exception as e:
        print("Database update failed:", e)
        conn.rollback()
    finally:
        cursor.close()
        conn.close()

def process_articles():
    articles = get_articles()
    grouped_articles = group_articles(articles)
    update_incident_mappings(grouped_articles, articles)

# Call this function to process the articles
#process_articles()

def delete_entries():
    delete_table('mapping')
    delete_table('articles')
    delete_table('incidents')
    create_articles_table()
    create_incidents_table()
    create_mapping_table()

#delete_entries()

#insert_mapping(1, 2)


