pub mod input_data;

fn main() {
    
    let init_temp = 298.15;
    let input_data = input_data::create_data(init_temp);
    let file_path = "building_data.json";
    let place = "Hervanta";

    let building_data = input_data::BuildingData::new(place.to_string(), input_data);
    
    input_data::write_to_json_file_bd(&building_data, file_path);

}
