use chrono::TimeDelta;
use juniper::GraphQLObject;
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Deserialize, GraphQLObject, PartialEq, Serialize)]
#[graphql(description = "Optimization time line settings.")]
pub struct TimeLineSettings {
    #[graphql(description = "Time line duration.")]
    duration: Duration,
    #[graphql(description = "Time step length.")]
    step: Duration,
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
        }
    }
}

impl TimeLineSettings {
    pub fn try_new(duration: Duration, step: Duration) -> Result<Self, String> {
        let time_line = TimeLineSettings { duration, step };
        time_line.validate()?;
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
        Ok(())
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

#[cfg(test)]
mod tests {
    use super::*;
    use std::error::Error;
    #[test]
    fn constructs_time_line_correctly() {
        let duration = Duration::try_new(13, 0, 0).expect("constructing duration should succeed");
        let step = Duration::try_new(0, 15, 0).expect("constructing step should succeed");
        let time_line = TimeLineSettings::try_new(duration, step)
            .expect("time line construction should succeed");
        assert_eq!(time_line.duration().to_time_delta(), TimeDelta::hours(13));
        assert_eq!(time_line.step().to_time_delta(), TimeDelta::minutes(15));
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
        if let Err(message) = TimeLineSettings::try_new(duration, step) {
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
        if let Err(message) = TimeLineSettings::try_new(duration, step) {
            assert_eq!(message, "time line step should not exceed duration");
        } else {
            return Err("validation should have failed".into());
        }
        Ok(())
    }
}
