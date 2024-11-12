use chrono::TimeDelta;
use config::builder::{ConfigBuilder, DefaultState};
use config::{Config, ConfigError};
use serde::de::{self, Visitor};
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use std::collections::HashMap;
use std::env;
use std::fmt;
use std::path;
use std::path::{Path, PathBuf};

pub const JULIA_EXEC_FIELD: &str = "julia_exec";
pub const PREDICER_RUNNER_PROJECT_FIELD: &str = "predicer_runner_project";
pub const PREDICER_PROJECT_FIELD: &str = "predicer_project";
pub const JULIA_DEFAULT_PROJECT: &str = "@";
pub const PYTHON_EXEC_FIELD: &str = "python_exec";
pub const TIME_LINE_FIELD: &str = "time_line";

#[derive(Clone, Deserialize, Serialize)]
pub struct Settings {
    pub julia_exec: String,
    pub predicer_runner_project: String,
    #[serde(default = "default_predicer_project")]
    pub predicer_project: String,
    #[serde(skip, default = "default_predicer_runner_script")]
    pub predicer_runner_script: String,
    #[serde(default = "default_predicer_port")]
    pub predicer_port: u16,
    pub python_exec: String,
    #[serde(skip, default = "default_weather_fetcher_script")]
    pub weather_fetcher_script: String,
    #[serde(default)]
    pub time_line: TimeLineSettings,
    pub location: Option<LocationSettings>,
}

impl Default for Settings {
    fn default() -> Self {
        Settings {
            julia_exec: String::new(),
            predicer_runner_project: JULIA_DEFAULT_PROJECT.to_string(),
            predicer_project: default_predicer_project(),
            predicer_runner_script: default_predicer_runner_script(),
            predicer_port: default_predicer_port(),
            python_exec: String::new(),
            weather_fetcher_script: default_weather_fetcher_script(),
            time_line: TimeLineSettings::default(),
            location: None,
        }
    }
}

fn default_predicer_project_path() -> PathBuf {
    PathBuf::from("Predicer")
}

fn default_predicer_project() -> String {
    path_to_string(
        &path::absolute(default_predicer_project_path())
            .expect("failed to resolve current directory"),
    )
}

fn default_predicer_runner_path() -> PathBuf {
    ["src", "Pr_ArrowConnection.jl"].iter().collect::<PathBuf>()
}

fn default_predicer_runner_script() -> String {
    default_predicer_runner_path()
        .to_str()
        .expect("predicer runner script path contains unknown characters")
        .to_string()
}

fn default_predicer_port() -> u16 {
    0
}

fn default_weather_fetcher_script() -> String {
    let path = ["src", "fmi_opendata.py"].iter().collect::<PathBuf>();
    path.to_str()
        .expect("weather fetcher script path contains unknown characters")
        .to_string()
}

fn path_to_string(path: &PathBuf) -> String {
    path.to_str()
        .expect("path should be valid string")
        .to_string()
}

pub fn map_from_environment_variables() -> HashMap<String, String> {
    let mut map = HashMap::<String, String>::new();
    for (key, value) in env::vars() {
        map.insert(key, value);
    }
    map
}

pub fn make_settings_file_path() -> PathBuf {
    directories::ProjectDirs::from("", "", "hertta")
        .expect("system should have a home directory")
        .preference_dir()
        .join("settings.toml")
}

fn make_config_builder(
    environment_variables: &HashMap<String, String>,
) -> ConfigBuilder<DefaultState> {
    let config = Config::builder()
        .set_default(PREDICER_RUNNER_PROJECT_FIELD, JULIA_DEFAULT_PROJECT)
        .expect("failed to add default Julia project to config builder");
    let julia_exec_from_path = environment_variables
        .get("PATH")
        .map_or_else(String::new, |paths| {
            exec_from(&paths, "julia").map_or_else(String::new, |path| path_to_string(&path))
        });
    let config = config
        .set_default(JULIA_EXEC_FIELD, julia_exec_from_path)
        .expect("failed to add default Julia executable to config builder")
        .set_default(PREDICER_PROJECT_FIELD, default_predicer_project())
        .expect("failed to add default Predicer project to config builder");
    let python_exec = match env::consts::OS {
        "windows" => "python",
        _ => "python3",
    };
    let python_exec_from_path =
        environment_variables
            .get("PATH")
            .map_or_else(String::new, |paths| {
                exec_from(&paths, python_exec)
                    .map_or_else(String::new, |path| path_to_string(&path))
            });
    config
        .set_default(PYTHON_EXEC_FIELD, python_exec_from_path)
        .expect("failed to add default Julia executable to config builder")
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
                .expect("invalid settings file path"),
            config::FileFormat::Toml,
        )
        .required(false),
    );
    let config = builder.build()?;
    config.try_deserialize::<Settings>()
}

fn exec_from(path_variable: &str, exec: &str) -> Option<PathBuf> {
    for path in env::split_paths(&path_variable) {
        let mut path = path.join(exec);
        path.set_extension(env::consts::EXE_EXTENSION);
        if path.exists() {
            return Some(path);
        }
    }
    None
}

pub fn validate_settings(settings: &Settings) -> Result<(), String> {
    if settings.julia_exec.is_empty() {
        return Err(String::from(
            "Julia executable not found in PATH or settings file",
        ));
    }
    let julia_exec_path = Path::new(&settings.julia_exec);
    if !julia_exec_path.exists() || !julia_exec_path.is_file() {
        return Err(format!(
            "invalid path to Julia executable '{}'",
            &settings.julia_exec
        ));
    }
    if settings.predicer_runner_project != JULIA_DEFAULT_PROJECT {
        if !Path::new(&settings.predicer_runner_project).exists() {
            return Err(format!(
                "invalid Predicer runner project '{}'",
                &settings.predicer_runner_project
            ));
        }
    }
    if !Path::new(&settings.predicer_project).exists() {
        return Err(format!(
            "invalid path to Predicer project '{}'",
            settings.predicer_project
        ));
    }
    if !Path::new(&settings.predicer_runner_script).exists() {
        return Err(format!(
            "invalid path to Predicer runner script '{}'",
            &settings.predicer_runner_script
        ));
    }
    if !Path::new(&settings.weather_fetcher_script).exists() {
        return Err(format!(
            "invalid path to weather fetcher script '{}'",
            &settings.weather_fetcher_script
        ));
    }
    Ok(())
}

#[derive(Clone, Deserialize, Serialize)]
pub struct TimeLineSettings {
    #[serde(
        deserialize_with = "deserialize_time_delta",
        serialize_with = "serialize_time_delta"
    )]
    pub step: TimeDelta,
    #[serde(
        deserialize_with = "deserialize_time_delta",
        serialize_with = "serialize_time_delta"
    )]
    pub duration: TimeDelta,
}

impl Default for TimeLineSettings {
    fn default() -> Self {
        TimeLineSettings {
            step: TimeDelta::minutes(15),
            duration: TimeDelta::hours(4),
        }
    }
}

fn deserialize_time_delta<'de, D>(deserializer: D) -> Result<TimeDelta, D::Error>
where
    D: Deserializer<'de>,
{
    struct MillisecondVisitor;
    impl<'d> Visitor<'d> for MillisecondVisitor {
        type Value = i64;
        fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
            formatter.write_str("a duration in milliseconds")
        }
        fn visit_i64<E: de::Error>(self, value: i64) -> Result<Self::Value, E> {
            if value > 0 {
                Ok(value)
            } else {
                Err(E::custom("non-positive duration".to_string()))
            }
        }
    }
    Ok(TimeDelta::milliseconds(
        deserializer.deserialize_i64(MillisecondVisitor)?,
    ))
}

fn serialize_time_delta<S: Serializer>(
    duration: &TimeDelta,
    serializer: S,
) -> Result<S::Ok, S::Error> {
    serializer.serialize_i64(duration.num_milliseconds())
}

#[derive(Clone, Deserialize, Serialize)]
pub struct LocationSettings {
    pub country: String,
    pub place: String,
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::error::Error;
    use std::ffi::OsStr;
    use std::fs::File;
    use std::path::Path;
    use tempfile;
    use tempfile::TempDir;

    #[test]
    fn settings_file_name_is_correct() {
        let path = make_settings_file_path();
        assert_eq!(path.file_name(), Some(OsStr::new("settings.toml")));
    }

    fn create_temporary_julia_exe() -> TempDir {
        let temp_dir =
            tempfile::tempdir().expect("temporary directory creation should be possible");
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
        assert!(exec_from(&path_string, "julia").is_none());
    }
    #[test]
    fn julia_found_in_path_when_its_executable_exists() {
        let temp_julia_dir = create_temporary_julia_exe();
        let path_string = env::join_paths([Path::new("/julia/is/not/here"), temp_julia_dir.path()])
            .expect("paths should be joinable")
            .into_string()
            .expect("test PATH should be a valid string");
        assert_eq!(
            exec_from(&path_string, "julia").expect("Julia should have been in PATH"),
            julia_exec_path_from(&temp_julia_dir)
        );
    }
    #[test]
    fn default_settings_when_nothing_is_provided() {
        let settings =
            make_settings(&HashMap::new(), &PathBuf::new()).expect("settings should work fine");
        assert!(settings.julia_exec.is_empty());
        assert_eq!(settings.predicer_runner_project, JULIA_DEFAULT_PROJECT);
        assert_eq!(settings.predicer_project, default_predicer_project());
        assert_eq!(
            settings.predicer_runner_script,
            default_predicer_runner_script()
        );
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
            path_to_string(&julia_exec_path_from(&temp_julia_dir))
        );
        assert_eq!(settings.predicer_runner_project, JULIA_DEFAULT_PROJECT);
        assert_eq!(settings.predicer_project, default_predicer_project());
        assert_eq!(
            settings.predicer_runner_script,
            default_predicer_runner_script()
        );
    }
    #[test]
    fn non_existent_julia_exec_is_caught() -> Result<(), Box<dyn Error>> {
        let settings = Settings::default();
        match validate_settings(&settings) {
            Ok(..) => Err("expected settings to be invalid".to_string().into()),
            Err(error) => {
                assert_eq!(
                    error,
                    "Julia executable not found in PATH or settings file".to_string()
                );
                Ok(())
            }
        }
    }
    #[test]
    fn julia_exec_pointint_to_directory_is_caught() -> Result<(), Box<dyn Error>> {
        let mut settings = Settings::default();
        let temp_dir = tempfile::tempdir().expect("failed to create temporary directory");
        settings.julia_exec = path_to_string(&PathBuf::from(temp_dir.path()));
        match validate_settings(&settings) {
            Ok(..) => Err("expected settings to be invalid".to_string().into()),
            Err(error) => {
                assert_eq!(
                    error,
                    format!("invalid path to Julia executable '{}'", settings.julia_exec)
                );
                Ok(())
            }
        }
    }
    #[test]
    fn invalid_predicer_runner_project_is_caught() -> Result<(), Box<dyn Error>> {
        let temp_julia_dir = create_temporary_julia_exe();
        let julia_path_string = path_to_string(&julia_exec_path_from(&temp_julia_dir));
        let mut settings = Settings::default();
        settings.julia_exec = julia_path_string;
        settings.predicer_runner_project = String::new();
        match validate_settings(&settings) {
            Ok(..) => Err("expected settings to be invalid".to_string().into()),
            Err(error) => {
                assert_eq!(error, "invalid Predicer runner project ''".to_string());
                Ok(())
            }
        }
    }
    #[test]
    fn invalid_predicer_project_is_caughs() -> Result<(), Box<dyn Error>> {
        let temp_julia_dir = create_temporary_julia_exe();
        let julia_path_string = path_to_string(&julia_exec_path_from(&temp_julia_dir));
        let mut settings = Settings::default();
        settings.julia_exec = julia_path_string;
        settings.predicer_project = String::new();
        match validate_settings(&settings) {
            Ok(..) => Err("expected settings to be invalid".to_string().into()),
            Err(error) => {
                assert_eq!(error, "invalid path to Predicer project ''".to_string());
                Ok(())
            }
        }
    }
    #[test]
    fn invalid_predicer_runner_script_is_caught() -> Result<(), Box<dyn Error>> {
        let temp_julia_dir = create_temporary_julia_exe();
        let julia_path_string = path_to_string(&julia_exec_path_from(&temp_julia_dir));
        let mut settings = Settings::default();
        settings.julia_exec = julia_path_string;
        settings.predicer_runner_script = String::new();
        match validate_settings(&settings) {
            Ok(..) => Err("expected settings to be invalid".to_string().into()),
            Err(error) => {
                assert_eq!(
                    error,
                    "invalid path to Predicer runner script ''".to_string()
                );
                Ok(())
            }
        }
    }
    #[test]
    fn invalid_weather_fetcher_script_is_caught() -> Result<(), Box<dyn Error>> {
        let temp_julia_dir = create_temporary_julia_exe();
        let julia_path_string = path_to_string(&julia_exec_path_from(&temp_julia_dir));
        let mut settings = Settings::default();
        settings.julia_exec = julia_path_string;
        settings.weather_fetcher_script = String::new();
        match validate_settings(&settings) {
            Ok(..) => Err("expected settings to be invalid".to_string().into()),
            Err(error) => {
                assert_eq!(
                    error,
                    "invalid path to weather fetcher script ''".to_string()
                );
                Ok(())
            }
        }
    }
}
