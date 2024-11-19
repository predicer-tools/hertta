pub mod errors;
pub mod event_loop;
pub mod graphql;
pub mod input_data;
pub mod input_data_base;
pub mod settings;
pub mod utilities;

use chrono::{DateTime, FixedOffset};

pub type TimeStamp = DateTime<FixedOffset>;
pub type TimeLine = Vec<TimeStamp>;
