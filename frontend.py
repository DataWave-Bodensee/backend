from fastapi.testclient import TestClient
from main import app  # replace with the actual location of your FastAPI app

import requests

url = "http://127.0.0.1:8000/articles"  # URL of the FastAPI server

response = requests.get(url)

print("Response from server:")

# Print the response from the server
print(response.json())

