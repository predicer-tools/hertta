use crate::graphql::HerttaContext;
use crate::input_data_base::BaseInputData;
use crate::time_line_settings::TimeLineSettings;
use directories::ProjectDirs;
use juniper::GraphQLObject;
use serde::{Deserialize, Serialize};
use std::fs::{self, File};
use std::path::PathBuf;

#[derive(Clone, Default, Deserialize, GraphQLObject, Serialize)]
#[graphql(description = "Optimization model.", context = HerttaContext)]
pub struct Model {
    #[serde(default)]
    pub time_line: TimeLineSettings,
    pub input_data: BaseInputData,
}

pub fn make_model_file_path() -> PathBuf {
    ProjectDirs::from("", "", "hertta")
        .expect("system should have a home directory")
        .config_local_dir()
        .join("model.json")
}

pub fn read_model_from_file(file_path: &PathBuf) -> Result<Model, String> {
    let model_file = File::open(file_path)
        .or_else(|error| Err(format!("failed to open model file: {}", error)))?;
    let model = serde_json::from_reader(model_file)
        .or_else(|error| Err(format!("failed to parse model file: {}", error)))?;
    Ok(model)
}

pub fn write_model_to_file(model: &Model, file_path: &PathBuf) -> Result<(), String> {
    match file_path.parent() {
        Some(settings_dir) => fs::create_dir_all(settings_dir)
            .or_else(|_| Err("failed to create config directory".to_string()))?,
        None => return Err("settings file should have a parent directory".into()),
    };
    let model_file =
        File::create(file_path).or_else(|_| Err("failed to create model file".to_string()))?;
    serde_json::to_writer_pretty(model_file, model)
        .or_else(|_| Err("failed to write model to file".to_string()))?;
    Ok(())
}
