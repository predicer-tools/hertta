pub mod event_loop;
pub mod graphql;
mod input_data;
mod input_data_base;
pub mod model;
mod scenarios;
pub mod settings;
mod time_line_settings;

use chrono::{DateTime, Utc};

pub type TimeStamp = DateTime<Utc>;
pub type TimeLine = Vec<TimeStamp>;
