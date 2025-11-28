import requests

API_KEY = "AIzaSyCRzx6QJ9PwjKz9x4J7JwQVUDKMdD6LeVw"
SPREADSHEET_ID = "13IgEjPDT-oQlt-k3JWtAxBGbmfTPOXOvIS7gPcSrSso"
RANGE = "Main Data!A1:M10001"

url = f"https://sheets.googleapis.com/v4/spreadsheets/{SPREADSHEET_ID}/values/{RANGE}?key={API_KEY}"

response = requests.get(url)
data = response.json()

print(data)
values = data.get("values", [])

for row in values:
    print("\t".join(row))
