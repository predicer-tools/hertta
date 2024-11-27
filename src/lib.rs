pub mod errors;
pub mod event_loop;
pub mod graphql;
pub mod input_data;
pub mod input_data_base;
pub mod model;
pub mod scenarios;
pub mod settings;
pub mod status;
pub mod time_line_settings;

use chrono::{DateTime, FixedOffset};

pub type TimeStamp = DateTime<FixedOffset>;
pub type TimeLine = Vec<TimeStamp>;
