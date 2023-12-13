#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Open Data collection for weather observations- WFS.

- Collect data over wfs for one or more locations
- 4 options listed for defining locations, more locs can be added.
- Define wanted parameters, 
        or to collect a default set of available parameters remove "parameters" argument
- Transforms output dict into viewable dataframe. 

@author: kalliov
"""
import argparse
import requests
import datetime as dt
import pandas as pd
from fmiopendata.wfs import download_stored_query
import json

pd.set_option('display.max_rows', 500)
pd.set_option('display.min_rows', 500)

def collect_data(start_time, end_time, place):
    # Collect open data
    collection_string = "fmi::observations::weather::multipointcoverage"

    # List the wanted observations
    parameters = ["Temperature",
                  "GLOB_PT1M_AVG",
                  "DIFF_PT1M_AVG",
                  "DIR_PT1M_AVG",
                  ]
    
    parameters_str = ','.join(parameters)
    
    # Latlons for collections (Places need to be station locations to have observations)
    #latlon_1 = "61.4481,23.8521"

    #latlon_2 = "60.16,24.93"


    # Option 2. Single place name
    snd = download_stored_query(collection_string,
                                args=["place="+ place,
                                      "starttime=" + start_time,
                                      "endtime=" + end_time,
                                      'parameters=' + parameters_str])

    data = snd.data
    return(data)

def reshape_dict(data, times):
    # Transform data output dict into stat-param-values form, with times separately
    # Dict to temporarily store data in new format
    new_data_dict = {}
    
    for time in times:
        # One timestep with all locations
        timestep_data = data[time]
        locations_list = list(timestep_data.keys())
        
        for location in locations_list:
            parameters = list(timestep_data[location].keys())
            # Add location and location dict into new data dict
            new_data_dict.setdefault(location, {})
        
            for param_name in parameters:
                # Take only parameter values (drop units)
                param_value = timestep_data[location][param_name]["value"]
                # Add parameter value of this timestep into list under param name
                new_data_dict[location].setdefault(param_name, []).append(param_value)
                
    return(new_data_dict, locations_list)

def main(start_time, end_time, place):
    # Retrieving open data for time period defined above. Parameters etc. are defined 
    # in the collect_data function.
    data = collect_data(start_time, end_time, place)
    
    # Times to use later in forming dataframe
    times = data.keys()
    
    # Reshape dict, return also loclist for visualisations
    new_data_dict, locations_list = reshape_dict(data, times)
    hourly_mean_temperatures = {}

    for location in locations_list:
        location_data = new_data_dict[location]
        df = pd.DataFrame(index=times, data=location_data)

        if 'Air temperature' in df.columns:
            # Resample by hour and calculate mean
            hourly_mean = df['Air temperature'].resample('H').mean()

            # Format Timestamps as strings and store the Series
            hourly_mean.index = hourly_mean.index.strftime('%Y-%m-%dT%H:%M:%S')
            hourly_mean_temperatures[location] = hourly_mean.to_dict()

    # Convert hourly_mean_temperatures to JSON and print
    json_output = json.dumps(hourly_mean_temperatures, indent=4)
    print(json_output)           


        
        

if __name__ == "__main__":
    # Set up argument parser
    parser = argparse.ArgumentParser(description='Open Data collection for weather observations')
    parser.add_argument('start_time', type=str, help='Start time for data collection in YYYY-MM-DD HH:MM format')
    parser.add_argument('end_time', type=str, help='End time for data collection in YYYY-MM-DD HH:MM format')
    parser.add_argument('place', type=str, help='Name of the place')

    # Parse arguments
    args = parser.parse_args()

    # Call main function with parsed arguments
    main(args.start_time, args.end_time, args.place)