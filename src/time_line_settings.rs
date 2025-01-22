use chrono::{TimeDelta, Utc, DurationRound};
use juniper::{GraphQLObject, GraphQLUnion, GraphQLEnum};
use serde::{Deserialize, Serialize};
use crate::TimeStamp;

#[derive(Clone, Debug, Deserialize, GraphQLObject, PartialEq, Serialize)]
#[graphql(description = "Optimization time line settings.")]
pub struct TimeLineSettings {
    #[graphql(description = "Time line duration.")]
    duration: Duration,
    #[graphql(description = "Time step length.")]
    step: Duration,
    #[graphql(description = "Start of the time line.")]
    start: TimeLineStart,
}

#[derive(Clone, Debug, Deserialize, Serialize, GraphQLUnion, PartialEq)]
#[graphql(description = "Defines the start of the time line.")]
pub enum TimeLineStart {
    ClockChoice(ClockChoice),
    CustomStartTime(CustomStartTime),
}

#[derive(Clone, Debug, Deserialize, Serialize, GraphQLObject, PartialEq)]
#[graphql(description = "Defines a clock-based start time.")]
pub struct ClockChoice {
    #[graphql(description = "Predefined clock option.")]
    pub choice: Clock,
}

#[derive(Clone, Debug, Deserialize, Serialize, GraphQLEnum, PartialEq)]
#[graphql(description = "Represents predefined clock options.")]
pub enum Clock {
    /// Use the current hour truncated to the nearest hour.
    CurrentHour,
}

impl Clock {
    pub fn calculate_start_time(&self) -> TimeStamp {
        match self {
            Clock::CurrentHour => Utc::now()
                .duration_trunc(chrono::Duration::hours(1))
                .expect("Truncation to nearest hour should succeed"),
        }
    }
}

#[derive(Clone, Debug, Deserialize, Serialize, GraphQLObject, PartialEq, Default)]
#[graphql(description = "Represents a user-defined start time.")]
pub struct CustomStartTime {
    #[graphql(description = "User-provided start time (ISO 8601).")]
    pub start_time: TimeStamp,
}

#[derive(Clone, Debug, Deserialize, Default, GraphQLObject, PartialEq, Serialize)]
pub struct Duration {
    hours: i32,
    minutes: i32,
    seconds: i32,
}

impl Duration {
    fn check_hours(hours: i32) -> Result<(), String> {
        if hours < 0 {
            return Err("hours should be non-negative".to_string());
        }
        Ok(())
    }
    fn check_minutes(minutes: i32) -> Result<(), String> {
        if minutes < 0 {
            return Err("minutes should be non-negative".to_string());
        }
        Ok(())
    }
    fn check_seconds(seconds: i32) -> Result<(), String> {
        if seconds < 0 {
            return Err("seconds should be non-negative".to_string());
        }
        Ok(())
    }
    pub fn try_new(hours: i32, minutes: i32, seconds: i32) -> Result<Self, String> {
        Self::check_hours(hours)?;
        Self::check_minutes(minutes)?;
        Self::check_seconds(seconds)?;
        Ok(Duration {
            hours,
            minutes,
            seconds,
        })
    }
    pub fn to_time_delta(&self) -> TimeDelta {
        return TimeDelta::hours(self.hours as i64)
            + TimeDelta::minutes(self.minutes as i64)
            + TimeDelta::seconds(self.seconds as i64);
    }
}

impl Default for TimeLineSettings {
    fn default() -> Self {
        TimeLineSettings {
            duration: Duration::try_new(4, 0, 0)
                .expect("constructing default duration should always succeed"),
            step: Duration::try_new(0, 15, 0)
                .expect("constructing default step should always succeed"),
            start: TimeLineStart::ClockChoice(ClockChoice {
                choice: Clock::CurrentHour,
            }),
        }
    }
}

impl TimeLineSettings {
    pub fn try_new(duration: Duration, step: Duration, start: TimeLineStart) -> Result<Self, String> {
        let time_line = TimeLineSettings { duration, step, start };
        time_line.validate()?; // Validate all parameters
        Ok(time_line)
    }
    fn validate(&self) -> Result<(), String> {
        let duration = self.duration.to_time_delta();
        if duration.num_hours() > 24 {
            return Err("time line duration should not exceed 24 hours".to_string());
        }
        let step = self.step.to_time_delta();
        if step > duration {
            return Err("time line step should not exceed duration".to_string());
        }
        if let TimeLineStart::CustomStartTime(ref custom_start) = self.start {
            if custom_start.start_time < Utc::now() {
                return Err("custom start time cannot be in the past.".to_string());
            }
        }
        Ok(())
    }
    pub fn start(&self) -> &TimeLineStart {
        &self.start
    }
    pub fn duration(&self) -> &Duration {
        &self.duration
    }
    pub fn set_duration(&mut self, duration: Duration) -> Result<(), String> {
        if duration.to_time_delta().num_hours() > 24 {
            return Err("time line duration should not exceed 24 hours".to_string());
        }
        self.duration = duration;
        Ok(())
    }
    pub fn step(&self) -> &Duration {
        &self.step
    }
    pub fn set_step(&mut self, step: Duration) -> Result<(), String> {
        let duration = self.duration.to_time_delta();
        if step.to_time_delta() > duration {
            return Err("time line step should not exceed duration".to_string());
        }
        self.step = step;
        Ok(())
    }
}

pub fn compute_timeline_start(time_line_settings: &TimeLineSettings) -> TimeStamp {
    match time_line_settings.start() {
        TimeLineStart::ClockChoice(clock_choice) => {
            clock_choice.choice.calculate_start_time()
        }
        TimeLineStart::CustomStartTime(custom_start) => {
            custom_start.start_time
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::error::Error;
    use chrono::{Utc, TimeZone};
    
    #[test]
    fn constructs_time_line_correctly_with_current_hour() {
        let duration = Duration::try_new(13, 0, 0).expect("constructing duration should succeed");
        let step = Duration::try_new(0, 15, 0).expect("constructing step should succeed");
        let start = TimeLineStart::ClockChoice(ClockChoice {
            choice: Clock::CurrentHour,
        });
        let time_line = TimeLineSettings::try_new(duration, step, start)
            .expect("time line construction should succeed");
        assert_eq!(time_line.duration().to_time_delta(), TimeDelta::hours(13));
        assert_eq!(time_line.step().to_time_delta(), TimeDelta::minutes(15));
        if let TimeLineStart::ClockChoice(clock_choice) = time_line.start() {
            assert_eq!(clock_choice.choice, Clock::CurrentHour);
        } else {
            panic!("Expected ClockChoice for start.");
        }
    }

    #[test]
    fn rejects_past_custom_start_time() {
        let duration = Duration::try_new(4, 0, 0).expect("constructing duration should succeed");
        let step = Duration::try_new(0, 15, 0).expect("constructing step should succeed");
        let custom_start_time = CustomStartTime {
            start_time: Utc.with_ymd_and_hms(2020, 1, 1, 12, 0, 0).unwrap(),
        };
        let start = TimeLineStart::CustomStartTime(custom_start_time);
        if let Err(message) = TimeLineSettings::try_new(duration, step, start) {
            assert_eq!(message, "custom start time cannot be in the past.");
        } else {
            panic!("Validation should have failed for past custom start time.");
        }
    }

    #[test]
    fn cannot_construct_non_positive_durations() {
        match Duration::try_new(-1, 0, 0) {
            Err(message) => assert_eq!(message, "hours should be non-negative"),
            Ok(..) => panic!("validation should have failed"),
        }
        match Duration::try_new(0, -1, 0) {
            Err(message) => assert_eq!(message, "minutes should be non-negative"),
            Ok(..) => panic!("validation should have failed"),
        }
        match Duration::try_new(0, 0, -1) {
            Err(message) => assert_eq!(message, "seconds should be non-negative"),
            Ok(..) => panic!("validation should have failed"),
        }
    }
    #[test]
    fn rejects_too_long_durations() -> Result<(), Box<dyn Error>> {
        let duration = Duration::try_new(25, 0, 0).expect("constructing duration should succeed");
        let step = Duration::try_new(0, 15, 0).expect("constructing step should succeed");
        let start = TimeLineStart::ClockChoice(ClockChoice {
            choice: Clock::CurrentHour,
        });
        if let Err(message) = TimeLineSettings::try_new(duration, step, start) {
            assert_eq!(message, "time line duration should not exceed 24 hours");
        } else {
            return Err("validation should have failed".into());
        }
        Ok(())
    }
    #[test]
    fn rejects_steps_that_are_longer_than_duration() -> Result<(), Box<dyn Error>> {
        let duration = Duration::try_new(4, 0, 0).expect("constructing duration should succeed");
        let step = Duration::try_new(5, 0, 0).expect("constructing step should succeed");
        let start = TimeLineStart::ClockChoice(ClockChoice {
            choice: Clock::CurrentHour,
        });
        if let Err(message) = TimeLineSettings::try_new(duration, step, start) {
            assert_eq!(message, "time line step should not exceed duration");
        } else {
            return Err("validation should have failed".into());
        }
        Ok(())
    }
}
