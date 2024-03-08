#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from http.server import BaseHTTPRequestHandler, HTTPServer
import urllib.parse as urlparse
import json
import datetime as dt
import pandas as pd
from fmiopendata.wfs import download_stored_query
import requests

# Set display options for pandas
pd.set_option('display.max_rows', 500)
pd.set_option('display.min_rows', 500)

class WeatherHandler(BaseHTTPRequestHandler):

    def do_GET(self):
        # Parse the URL path and query components
        parsed_path = urlparse.urlparse(self.path)
        path = parsed_path.path
        query = parsed_path.query
        query_components = urlparse.parse_qs(query)

        # Handling the '/get_weather_data' endpoint
        if path == '/get_weather_data':
            start_time = query_components.get('start_time', [None])[0]
            end_time = query_components.get('end_time', [None])[0]
            place = query_components.get('place', [None])[0]

            # Check for missing parameters
            if None in (start_time, end_time, place):
                self.send_response(400)
                self.end_headers()
                self.wfile.write(b'Bad Request: Missing parameters')
            else:
                # Collect and reshape data to return only the values as a list of f64
                data = collect_data(start_time, end_time, place)
                values_list = reshape_dict(data, list(data.keys()))

                # Construct the response with just the values
                response_data = {
                    'place': place,
                    'weather_values': values_list
                }

                # Send the response
                self.send_response(200)
                self.send_header('Content-type', 'application/json')
                self.end_headers()
                self.wfile.write(json.dumps(response_data).encode())
        else:
            # Handle unknown paths
            self.send_response(404)
            self.end_headers()
            self.wfile.write(b'Not Found')

def run_server(server_class=HTTPServer, handler_class=WeatherHandler):
    server_address = ('', 8001)  # Host on all available interfaces on port 8000
    httpd = server_class(server_address, handler_class)
    print('Starting httpd...')
    httpd.serve_forever()

# Functions collect_data and reshape_dict remain the same as in your original script
    
def collect_data(start_time, end_time, place):
    collection_string = "fmi::forecast::harmonie::surface::point::multipointcoverage"
    parameters = ["Temperature"]
    parameters_str = ','.join(parameters)

    snd = download_stored_query(collection_string,
                                args=["place=" + place,
                                        "starttime=" + start_time,
                                        "endtime=" + end_time,
                                        'parameters=' + parameters_str])

    return snd.data

def reshape_dict(data, times):
    values_list = []
    for time in times:
        for location_data in data[time].values():
            for param_data in location_data.values():
                value = param_data["value"]
                if isinstance(value, (float, int)):  # Ensure the value is numeric
                    values_list.append(value)
    return values_list


if __name__ == '__main__':
    run_server()