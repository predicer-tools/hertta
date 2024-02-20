from http.server import BaseHTTPRequestHandler, HTTPServer
from urllib.parse import parse_qs
import requests
import json
from datetime import datetime
from requests.exceptions import HTTPError
import asyncio
import time
import os
import yaml

class MyHandler(BaseHTTPRequestHandler):
    # Initialize class variables for options
    listen_ip = '127.0.0.1'
    port = 8000
    hass_token = None

    def do_POST(self):
        # Extract POST request data.
        content_length = int(self.headers['Content-Length'])
        post_data = self.rfile.read(content_length).decode('utf-8')

        if self.path == '/from_hass/post':
            # Test print the received data.
            print(f'HASS backend received POST data from /from_hass/post: {post_data}'.encode('utf-8'))
            
            # Send a response back to the client.
            self.send_response(200)
            self.send_header('Content-type', 'text/plain')
            self.end_headers()
            self.wfile.write(f'Received POST data from /from_hass/post: {post_data}'.encode('utf-8'))
                        
            # Forward the received message to another server.
            forwarding_url = 'http://127.0.0.1:8002/from_hass/post'
            forwarding_headers = {'Content-type': 'application/json'}
            forwarded = self.make_post_request(forwarding_url, forwarding_headers, post_data)
            
            if forwarded:
                # Send a response back to the client
                self.send_response(200)
                self.send_header('Content-type', 'text/plain')
                self.end_headers()
                self.wfile.write(f'Received and forwarded POST data from /from_hass/post: {post_data}'.encode('utf-8'))
            else:
                # Failed to forward the message
                self.send_response(500)
                self.send_header('Content-type', 'text/plain')
                self.end_headers()
                self.wfile.write(b'Failed to forward the message')
        elif self.path == '/to_hass/post':
            # Test print the received data.
            print(f'HASS backend received POST data from /to_hass/post: {post_data}'.encode('utf-8'))
            
            # Send a response back to the client
            self.send_response(200)
            self.send_header('Content-type', 'text/plain')
            self.end_headers()
            self.wfile.write(f'Received POST data from /to_hass/post: {post_data}'.encode('utf-8'))
            
            # Forward the received message to another server
            forwarding_url = 'http://127.0.0.1:8001/to_hass/post'
            forwarding_headers = {'Content-type': 'application/json'}
            forwarded = self.make_post_request(forwarding_url, forwarding_headers, post_data)
            
            if forwarded:
                # Send a response back to the client
                self.send_response(200)
                self.send_header('Content-type', 'text/plain')
                self.end_headers()
                self.wfile.write(f'Received and forwarded POST data from /to_hass/post: {post_data}'.encode('utf-8'))
            else:
                # Failed to forward the message
                self.send_response(500)
                self.send_header('Content-type', 'text/plain')
                self.end_headers()
                self.wfile.write(b'Failed to forward the message')
        elif self.path == '/from_hertta/optimization_results':
            self.handle_optimization_results(post_data)
        else:
            # Send a response back to the client
            self.send_response(404)
            self.send_header('Content-type', 'text/plain')
            self.end_headers()
            self.wfile.write(b'Not Found')

    @classmethod
    def handle_optimization_results(self, data):
        print(f'Received optimization results: {data}')
        # Here, process the received optimization data as needed
        # You might want to store it in a file, database, or perform other actions

        # Send a response back to the client
        self.send_response(200)
        self.send_header('Content-type', 'text/plain')
        self.end_headers()
        self.wfile.write(b'Optimization results received and processed')       
        
    @classmethod
    def make_post_request(cls, url, headers, data):
        try:
            # Send a POST request to the specified URL with headers and data
            response = requests.post(url, headers=headers, data=data)
            
            print("Response Content:", response.content)
            print("Status Code:", response.status_code)

            if response.status_code == 200:
                print("POST request successful")
                return True
            else:
                print(f"POST request failed with status code {response.status_code}")
                return False
        except Exception as e:
            print(f"An error occurred: {e}")
            return False
          
    @classmethod
    def make_post_request_control(cls, url, entity_id, token, value):
        # Validate the token
        if not cls.is_valid_http_header_value(token):
            print("Invalid token header")
            return False

        # Construct the headers
        headers = {
            'Content-Type': 'application/json',
            'Authorization': token
        }

        # Construct the JSON payload
        payload = {
            'entity_id': entity_id,
            'value': value
        }

        try:
            # Send the POST request
            response = requests.post(url, headers=headers, json=payload)
            response.raise_for_status()  # Raises HTTPError for 4XX/5XX status codes

            print("Response Content:", response.content)
            print("Status Code:", response.status_code)

            if response.status_code == 200:
                print("POST request successful")
                return True
            else:
                print(f"POST request failed with status code {response.status_code}")
                return False

        except HTTPError as http_err:
            print(f"HTTP error occurred: {http_err}")
            return False
        except Exception as e:
            print(f"An error occurred: {e}")
            return False

    @classmethod
    def is_valid_http_header_value(cls, value):
        # Implement your validation logic here
        # For example, check if the token is not empty and has a valid format
        return value is not None and isinstance(value, str) and len(value) > 0


    @classmethod
    def read_options(cls):
        try:
            with open('data/options.json', 'r') as options_file:
                # Save options data as class variables
                options = json.load(options_file)
                cls.listen_ip = options.get('listen_ip', '0.0.0.0')
                cls.port = int(options.get('port', 8000))
                cls.hass_token = options.get('hass_token')
            return options
        except FileNotFoundError:
            return {}

    @classmethod       
    async def send_control_commands(url, entity_id, hass_token, control_values):
        for value in control_values[:2]:  # Limiting to the first two values
            print(f"Setting {entity_id} value to: {value}")
            result = MyHandler.make_post_request_light(url, entity_id, hass_token, value)
            
            if not result:
                print(f"Error in making POST request for {value}")
                # Decide how to handle the error: pass to continue to the next iteration or handle differently
            else:
                print(f"POST request successful for value: {value}")

            # Wait for 2 seconds before sending the next request
            print("Waiting for 2 seconds before next request...")
            await asyncio.sleep(2)  # Async sleep

        return True
    
    def do_GET(self):
        if self.path == '/to_hertta/model_data':
            self.handle_model_data_request()
        else:
            # Handle other GET requests or return 404 Not Found
            self.send_response(404)
            self.end_headers()
            self.wfile.write(b'Not Found')

    def handle_model_data_request(self):
        # Construct an absolute path to the YAML file
        yaml_file_path = os.path.join(os.path.dirname(__file__), 'model_data.yaml')

        try:
            with open(yaml_file_path, 'r') as file:
                model_data = yaml.safe_load(file)
            
            self.send_response(200)
            self.send_header('Content-type', 'application/x-yaml')
            self.end_headers()
            self.wfile.write(yaml.dump(model_data).encode())
        except FileNotFoundError:
            self.send_response(404)
            self.end_headers()
            self.wfile.write(b'Model data not found')
        except Exception as e:
            self.send_response(500)
            self.end_headers()
            self.wfile.write(str(e).encode())

def run_server(server_class=HTTPServer, handler_class=MyHandler):
    MyHandler.read_options()  # Read options before starting
    server_address = (MyHandler.listen_ip, MyHandler.port)
    httpd = server_class(server_address, handler_class)
    print(f'Starting the server on {MyHandler.listen_ip}:{MyHandler.port}.')
    httpd.serve_forever()

# Start the server.
run_server()