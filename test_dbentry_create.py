from webscraper.webscraper import *
from database.db_operations import insert_article

#scrape_and_save()
#articles = load_articles()
#filter_on_keywords_and_save(articles)
#articles = load_filtered_on_keywords_articles()
#filter_on_llm_and_save(articles)
articles = load_filtered_on_llm_articles()
write_to_db(articles)