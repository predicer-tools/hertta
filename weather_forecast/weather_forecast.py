#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from http.server import BaseHTTPRequestHandler, HTTPServer
import urllib.parse as urlparse
import json
import datetime as dt
import pandas as pd
from fmiopendata.wfs import download_stored_query

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
                # Collect and reshape data
                data = collect_data(start_time, end_time, place)
                times = list(data.keys())
                new_data_dict, locations_list = reshape_dict(data, times)

                # Convert data to match the Rust WeatherData struct
                for location in locations_list:
                    # Convert DataFrame to a list of tuples (timestamp, value)
                    df = pd.DataFrame(index=times, data=new_data_dict[location]).reset_index()
                    # Convert Timestamp to string
                    df['index'] = df['index'].astype(str)
                    weather_data = list(df.itertuples(index=False, name=None))

                    # Construct the response
                    response_data = {
                        'place': location,
                        'weather_data': weather_data
                    }

                    # Send the response
                    self.send_response(200)
                    self.send_header('Content-type', 'application/json')
                    self.end_headers()
                    self.wfile.write(json.dumps(response_data).encode())
                    return  # Important to return after sending the response

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
    new_data_dict = {}
    for time in times:
        timestep_data = data[time]
        locations_list = list(timestep_data.keys())
        
        for location in locations_list:
            parameters = list(timestep_data[location].keys())
            new_data_dict.setdefault(location, {})
        
            for param_name in parameters:
                param_value = timestep_data[location][param_name]["value"]
                new_data_dict[location].setdefault(param_name, []).append(param_value)
                
    return new_data_dict, locations_list


if __name__ == '__main__':
    run_server()


