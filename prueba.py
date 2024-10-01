import requests
import json

api_key = "0E6O_kbTiqLJalWtmJmlGpTztFUFmmFR"
symbol = "EUR-USD"
url = f"https://api.polygon.io/v2/aggs/ticker/C:{symbol.replace('-', '')}/range/1/hour/2023-09-01/2023-09-29?apiKey={api_key}"

response = requests.get(url)
data = response.json()

print(json.dumps(data, indent=4))
