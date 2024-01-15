pub mod input_data;

fn main() {
    
    let init_temp = 298.15;
    let input_data = input_data::create_data(init_temp);
    let file_path = "data.json";
    let place = "Hervanta";

    let weather_data_instance = input_data::WeatherData {
        place: String::from("Hervanta"),
        weather_data: vec![
            (String::from("2024-01-16 00:00:00"), -19.3),
            (String::from("2024-01-16 01:00:00"), -19.9),
            (String::from("2024-01-16 02:00:00"), -20.0),
            (String::from("2024-01-16 03:00:00"), -19.7),
            (String::from("2024-01-16 04:00:00"), -19.6),
            (String::from("2024-01-16 05:00:00"), -19.3),
            (String::from("2024-01-16 06:00:00"), -19.4),
            (String::from("2024-01-16 07:00:00"), -19.6),
            (String::from("2024-01-16 08:00:00"), -19.6),
        ],
    };

    let building_data = input_data::BuildingData::new(place.to_string(), input_data);

    let optimization_data = input_data::OptimizationData {
        weather_data: weather_data_instance,
        device_data: building_data,
    };
    
    input_data::write_to_json_file_od(&optimization_data, file_path);

}
