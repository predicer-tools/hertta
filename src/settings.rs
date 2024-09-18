use config::builder::{ConfigBuilder, DefaultState};
use config::{Config, ConfigError};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::env;
use std::path::PathBuf;

const JULIA_EXEC_FIELD: &str = "julia_exec";
const JULIA_PROJECT_FIELD: &str = "julia_project";

#[derive(Serialize, Deserialize)]
pub struct Settings {
    pub julia_exec: Option<String>,
    pub julia_project: String,
}

pub fn map_from_environment_variables() -> HashMap<String, String> {
    let mut map = HashMap::<String, String>::new();
    for (key, value) in env::vars() {
        map.insert(key, value);
    }
    map
}

pub fn make_settings_file_path() -> PathBuf {
    directories::ProjectDirs::from("", "", "Hertta")
        .expect("system should have a home directory")
        .preference_dir()
        .join("settings.toml")
}

fn make_config_builder(
    environment_variables: &HashMap<String, String>,
) -> ConfigBuilder<DefaultState> {
    let config = Config::builder()
        .set_default(JULIA_PROJECT_FIELD, "@")
        .expect("key should be convertible to string");
    let julia_exec_from_path = environment_variables
        .get("PATH")
        .and_then(|paths| julia_exec_from(&paths).and_then(|path| path.to_str().map(String::from)));
    config
        .set_default(JULIA_EXEC_FIELD, julia_exec_from_path)
        .expect("key should be convertible to string")
}

pub fn make_settings(
    environment_variables: &HashMap<String, String>,
    settings_file_path: &PathBuf,
) -> Result<Settings, ConfigError> {
    let builder = make_config_builder(environment_variables);
    let builder = builder.add_source(
        config::File::new(
            settings_file_path
                .to_str()
                .expect("file path should be convertible to string"),
            config::FileFormat::Toml,
        )
        .required(false),
    );
    let config = builder.build()?;
    config.try_deserialize::<Settings>()
}

fn julia_exec_from(path_variable: &str) -> Option<PathBuf> {
    for path in env::split_paths(&path_variable) {
        let mut path = path.join("julia");
        path.set_extension(env::consts::EXE_EXTENSION);
        if path.exists() {
            return Some(path);
        }
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::ffi::OsStr;
    use std::fs::File;
    use std::path::Path;
    use tempfile::{tempdir, TempDir};

    #[test]
    fn settings_file_name_is_correct() {
        let path = make_settings_file_path();
        assert_eq!(path.file_name(), Some(OsStr::new("settings.toml")));
    }

    fn create_temporary_julia_exe() -> TempDir {
        let temp_dir = tempdir().expect("temporary directory creation should be possible");
        let julia_exec_path = julia_exec_path_from(&temp_dir);
        File::create(&julia_exec_path).expect("file creation should succeed");
        temp_dir
    }

    fn julia_exec_path_from(temp_dir: &TempDir) -> PathBuf {
        let mut julia_exec_path = temp_dir.path().join("julia");
        julia_exec_path.set_extension(env::consts::EXE_EXTENSION);
        julia_exec_path
    }

    #[test]
    fn julia_not_found_in_path_means_none() {
        let path_string = env::join_paths([
            Path::new("/julia/is/not/here"),
            Path::new("/it/is/not/here/either"),
        ])
        .expect("paths should be joinable")
        .into_string()
        .expect("test PATH should be valid string");
        assert!(julia_exec_from(&path_string).is_none());
    }
    #[test]
    fn julia_found_in_path_when_its_executable_exists() {
        let temp_julia_dir = create_temporary_julia_exe();
        let path_string = env::join_paths([Path::new("/julia/is/not/here"), temp_julia_dir.path()])
            .expect("paths should be joinable")
            .into_string()
            .expect("test PATH should be a valid string");
        assert_eq!(
            julia_exec_from(&path_string).expect("Julia should have been in PATH"),
            julia_exec_path_from(&temp_julia_dir)
        );
    }
    #[test]
    fn default_settings_when_nothing_is_provided() {
        let settings =
            make_settings(&HashMap::new(), &PathBuf::new()).expect("settings should work fine");
        assert_eq!(settings.julia_exec, None);
        assert_eq!(settings.julia_project, "@");
    }
    #[test]
    fn default_settings_when_julia_is_in_path() {
        let temp_julia_dir = create_temporary_julia_exe();
        let path_string = env::join_paths([temp_julia_dir.path()])
            .expect("paths should be joinable")
            .into_string()
            .expect("test PATH should be a valid string");
        let mut environment_variables = HashMap::<String, String>::new();
        environment_variables.insert(String::from("PATH"), path_string);
        let settings = make_settings(&environment_variables, &PathBuf::new())
            .expect("settings should work fine");
        assert_eq!(
            settings.julia_exec,
            julia_exec_path_from(&temp_julia_dir)
                .to_str()
                .map(String::from)
        );
        assert_eq!(settings.julia_project, "@");
    }
}
