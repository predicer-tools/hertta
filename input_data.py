import requests
import json

# Define the URL and query parameters
url = "http://127.0.0.1:3030/api/optimize"
params = {
    "fetch_time_data": "false",
    "fetch_weather_data": "false",
    "fetch_elec_data": "",
    "country": "fi",
    "location": "Hervanta"
}
headers = {
    "Content-Type": "application/json"
}

# Read JSON data from file
with open('tests/predicer/predicer_no_nodehistories.json', 'r') as file:
    data = json.load(file)

# Send the POST request
response = requests.post(url, params=params, headers=headers, data=json.dumps(data))

# Print the response status code and content
print(f"Status Code: {response.status_code}")

try:
    response_json = response.json()
    print(f"Response: {response_json}")
except json.JSONDecodeError:
    print("Error: Unable to parse response as JSON")
    print(f"Response content: {response.text}")
