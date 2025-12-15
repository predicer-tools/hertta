# /// script
# dependencies = [
#   "fmiopendata",
#   "numpy",
#   "pandas",
# ]
# ///
import argparse
from datetime import datetime
import json

import pandas as pd
import numpy as np
from fmiopendata.wfs import download_stored_query


def collect_data(start_time: str, end_time: str, place: str):
    collection_string = "fmi::forecast::harmonie::surface::point::multipointcoverage"
    parameters = ["Temperature"]
    parameters_str = ','.join(parameters)
    snd = download_stored_query(collection_string,
                                args=["place="+ place,
                                      "starttime=" + start_time,
                                      "endtime=" + end_time,
                                      'parameters=' + parameters_str])
    data = snd.data
    return data


def reshape_dict(data: dict[datetime, dict[str, dict[str, dict]]]) -> dict[str, dict[str, np.float64]]:
    # Transform data output dict into stat-param-values form, with times separately
    new_data_dict = {}
    for timestep_data in data.values():
        for location, parameters in timestep_data.items():
            param_dict = new_data_dict.setdefault(location, {})
            for param_name, param_value in parameters.items():
                value = param_value["value"]
                param_dict.setdefault(param_name, []).append(value)
    return new_data_dict


def main(start_time: str, end_time: str, step: int, place: str) -> None:
    data = collect_data(start_time, end_time, place)
    reshaped_data = reshape_dict(data)
    df = pd.DataFrame(index=data.keys(), data=reshaped_data[place])
    df['Air temperature'] += 273.15
    mean_temperature = df['Air temperature'].resample(f'{step}min').nearest()
    mean_temperature.index = mean_temperature.index.strftime('%Y-%m-%dT%H:%M:%S')
    json_output = json.dumps([(time, temperature) for time, temperature in mean_temperature.to_dict().items()])
    print(json_output)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Open Data collection for weather observations')
    parser.add_argument('start_time', type=str, help='Start time for data collection in UTC and YYYY-MM-DD HH:MM format')
    parser.add_argument('end_time', type=str, help='End time for data collection in UTC and YYYY-MM-DD HH:MM format')
    parser.add_argument('step', type=int, help='Step between time stamps in minutes')
    parser.add_argument('place', type=str, help='Name of the place')
    args = parser.parse_args()
    main(args.start_time, args.end_time, args.step, args.place)
