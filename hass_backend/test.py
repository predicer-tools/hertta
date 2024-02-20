import requests

response = requests.get('http://localhost:8000/to_hertta/building_data')
if response.status_code == 200:
    print("Success!")
    print("YAML Data:\n", response.text)
else:
    print("Failed with status code:", response.status_code)
