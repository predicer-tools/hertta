#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Open Data collection for MEPS deterministic member - WFS.

- Collect data over wfs for one or more locations
- 4 options listed for defining locations, more locs can be added.
- Define wanted parameters, 
        or to collect all available parameters remove "parameters" argument
- Transforms output dict into viewable dataframe. 

@author: kalliov
"""

import requests
import datetime as dt
import pandas as pd
from fmiopendata.wfs import download_stored_query

pd.set_option('display.max_rows', 500)
pd.set_option('display.min_rows', 500)


def collect_data(start_time, end_time):
    # Collect open data
    collection_string = "fmi::forecast::harmonie::surface::point::multipointcoverage"

    # List the wanted MEPS parameters
    parameters = ["Temperature"
                  ]
    parameters_str = ','.join(parameters)
    # Latlons for collections
    latlon_1 = "60.169,24.938"
    latlon_2 = "60.16,24.93"
    
    # Option 1. Single latlon
    """snd = download_stored_query(collection_string,
                                args=["latlon=" + latlon_1,
                                      "starttime=" + start_time,
                                      "endtime=" + end_time,
                                      'parameters=' + parameters_str])"""
    # Option 2. Multiple latlons
    """snd = download_stored_query(collection_string,
                                args=["latlon=" + latlon_1,
                                      "latlon=" + latlon_2,
                                      "starttime=" + start_time,
                                      "endtime=" + end_time,
                                      'parameters=' + parameters_str])"""
    # Option 3. Single place name
    snd = download_stored_query(collection_string,
                                args=["place="+ "Hervanta",
                                      "starttime=" + start_time,
                                      "endtime=" + end_time,
                                      'parameters=' + parameters_str])
    # Option 4. Multiple place names
    """snd = download_stored_query(collection_string,
                                args=["place="+ "Kumpula",
                                      "place="+ "Helsinki",
                                      "starttime=" + start_time,
                                      "endtime=" + end_time,
                                      'parameters=' + parameters_str])"""

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

def main():
    
    # Set start and end time for forecast collection
    now = dt.datetime.utcnow()
    start_time = now.strftime('%Y-%m-%dT00:00:00Z')
    # Time_length (hours) includes potentially the beginning with no forecast
    # data available. i.e. its not equal to forecast length.
    length_of_period = 50
    end_time = now + dt.timedelta(hours=length_of_period)
    end_time = end_time.strftime('%Y-%m-%dT18:00:00Z')
    
    # Retrieving open data for times defined above. Parameters etc. are defined 
    # in the collect_data function.
    data = collect_data(start_time, end_time)
    
    # Times to use later in forming dataframe
    times = data.keys()
    
    # Reshape dict, return also loclist for visualisations
    new_data_dict, locations_list = reshape_dict(data, times)               
                
    # Make a dataframe for each station for visualisation        
    for location in locations_list:
        location_data = new_data_dict[location]
        print('\n' + location)
        df = pd.DataFrame(index=times, data=location_data)
        print(df)
        
        

if __name__ == "__main__":
    main()