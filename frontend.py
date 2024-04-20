from fastapi.testclient import TestClient
from main import app  # replace with the actual location of your FastAPI app

import requests

url = "http://127.0.0.1:8000/articles"  # URL of the FastAPI server

#response = requests.get(url)

print("Response from server:")


client = TestClient(app)

# Define the article data as a dictionary
article_data = {
    "website": "example.com",
    "content": "Lorem ipsum dolor sit amet, consectetur adipiscing elit.",
    "keywords": ["Lorem", "ipsum", "dolor"],
    "date": "2024-04-20",
    "number_dead": 10,
    "number_missing": 5,
    "number_survivors": 100,
    "country_of_origin": "Example Country",
    "region_of_origin": "Example Region",
    "cause_of_death": "Natural Disaster",
    "region_of_incident": "Example Region",
    "country_of_incident": "Example Country",
    "location_of_incident": "Example Location",
    "latitude": 0.0,
    "longitude": 0.0
}

# Make a POST request to the "/articles" endpoint with the article data
response = requests.post("http://localhost:8000/articles", json=article_data)

# Print the response from the server
print(response.json())

