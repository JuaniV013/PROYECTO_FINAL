import requests

url = "https://su-api-de-fastapi.com/api/endpoint" # Reemplace con su URL de API
response = requests.get(url)
datos = response.json()

print(datos)
