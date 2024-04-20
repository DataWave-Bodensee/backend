from webscraper.webscraper import *
from database.db_operations import insert_article

#scrape_and_save()
#articles = load_articles()
#filter_and_save(articles)
articles = load_filtered_articles()
for article in articles.itertuples():
    print("Inserting article {} from {}...".format(article.Index + 1, len(articles)))
    llm_create_db_entry(article)