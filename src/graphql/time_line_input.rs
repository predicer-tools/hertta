use super::{ValidationError, ValidationErrors};
use crate::time_line_settings::{Duration, TimeLineSettings, Clock, CustomStartTime, TimeLineStart, ClockChoice};
use juniper::GraphQLInputObject;
use chrono::{DateTime, Utc};

#[derive(GraphQLInputObject)]
pub struct DurationInput {
    hours: i32,
    minutes: i32,
    seconds: i32,
}

impl DurationInput {
    pub fn to_duration(self) -> Result<Duration, String> {
        Ok(Duration::try_new(self.hours, self.minutes, self.seconds)?)
    }
}

#[derive(GraphQLInputObject)]
pub struct TimeLineUpdate {
    duration: Option<DurationInput>,
    step: Option<DurationInput>,
    start: Option<TimeLineStartInput>,
}

#[derive(GraphQLInputObject)]
pub struct TimeLineStartInput {
    clock_choice: Option<Clock>,
    custom_start_time: Option<String>,
}

pub fn update_time_line(
    input: TimeLineUpdate,
    time_line: &mut TimeLineSettings,
) -> ValidationErrors {
    let mut errors = Vec::new();
    if let Some(duration_input) = input.duration {
        duration_from_input(
            duration_input,
            |d| time_line.set_duration(d),
            "duration",
            &mut errors,
        );
    }
    if let Some(step_input) = input.step {
        duration_from_input(step_input, |d| time_line.set_step(d), "step", &mut errors);
    }
    // Update timeline start if provided.  Prefer customStartTime over clockChoice.
    if let Some(start_input) = input.start {
        // Handle custom ISO‑8601 start time
        if let Some(start_str) = start_input.custom_start_time {
            match DateTime::parse_from_rfc3339(&start_str) {
                Ok(dt) => {
                    let dt_utc: DateTime<Utc> = dt.with_timezone(&Utc);
                    let custom_start = CustomStartTime { start_time: dt_utc };
                    let new_start = TimeLineStart::CustomStartTime(custom_start);
                    if let Err(error) = time_line.set_start(new_start) {
                        errors.push(ValidationError::new("customStartTime", &error));
                    }
                }
                Err(_) => {
                    errors.push(ValidationError::new(
                        "customStartTime",
                        &format!("invalid datetime format: {}", start_str),
                    ));
                }
            }
        }
        // Otherwise handle predefined clock choice
        else if let Some(clock) = start_input.clock_choice {
            let new_start = TimeLineStart::ClockChoice(ClockChoice { choice: clock });
            if let Err(error) = time_line.set_start(new_start) {
                errors.push(ValidationError::new("clockChoice", &error));
            }
        }
    }
    ValidationErrors::from(errors)
}

fn duration_from_input<F>(
    input: DurationInput,
    set_duration: F,
    field: &str,
    errors: &mut Vec<ValidationError>,
) where
    F: FnOnce(Duration) -> Result<(), String>,
{
    match input.to_duration() {
        Ok(duration) => match set_duration(duration) {
            Ok(..) => (),
            Err(error) => errors.push(ValidationError::new(field, &error)),
        },
        Err(error) => errors.push(ValidationError::new(field, &error)),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::time_line_settings::TimeLineSettings;
    use chrono::TimeDelta;

    #[test]
    fn update_time_line_works() {
        let mut time_line_settings = TimeLineSettings::default();
        let input = TimeLineUpdate {
            duration: Some(DurationInput {
                hours: 13,
                minutes: 0,
                seconds: 0,
            }),
            step: Some(DurationInput {
                hours: 0,
                minutes: 45,
                seconds: 0,
            }),
            start: None,
        };
        let errors = update_time_line(input, &mut time_line_settings);
        assert_eq!(errors.errors.len(), 0);
        assert_eq!(
            time_line_settings.duration().to_time_delta(),
            TimeDelta::hours(13)
        );
        assert_eq!(
            time_line_settings.step().to_time_delta(),
            TimeDelta::minutes(45)
        );
    }

    #[test]
    fn setting_negative_duration_causes_error() {
        let mut time_line_settings = TimeLineSettings::default();
        let input = TimeLineUpdate {
            duration: Some(DurationInput {
                hours: -13,
                minutes: 0,
                seconds: 0,
            }),
            step: Some(DurationInput {
                hours: 0,
                minutes: 45,
                seconds: 0,
            }),
            start: None,
        };
        let errors = update_time_line(input, &mut time_line_settings);
        assert_eq!(errors.errors.len(), 1);
        assert_eq!(errors.errors[0].field, "duration");
        assert_eq!(errors.errors[0].message, "hours should be non-negative");
        let mut expected_time_line = TimeLineSettings::default();
        expected_time_line
            .set_step(Duration::try_new(0, 45, 0).unwrap())
            .unwrap();
        assert_eq!(time_line_settings, expected_time_line);
    }

    #[test]
    fn setting_negative_step_causes_error() {
        let mut time_line_settings = TimeLineSettings::default();
        let input = TimeLineUpdate {
            duration: Some(DurationInput {
                hours: 13,
                minutes: 0,
                seconds: 0,
            }),
            step: Some(DurationInput {
                hours: 0,
                minutes: -45,
                seconds: 0,
            }),
            start: None,
        };
        let errors = update_time_line(input, &mut time_line_settings);
        assert_eq!(errors.errors.len(), 1);
        assert_eq!(errors.errors[0].field, "step");
        assert_eq!(errors.errors[0].message, "minutes should be non-negative");
        let mut expected_time_line = TimeLineSettings::default();
        expected_time_line
            .set_duration(Duration::try_new(13, 0, 0).unwrap())
            .unwrap();
        assert_eq!(time_line_settings, expected_time_line);
    }
    #[test]
    fn setting_step_longer_than_duration_causes_error() {
        let mut time_line_settings = TimeLineSettings::default();
        let input = TimeLineUpdate {
            duration: Some(DurationInput {
                hours: 13,
                minutes: 0,
                seconds: 0,
            }),
            step: Some(DurationInput {
                hours: 45,
                minutes: 0,
                seconds: 0,
            }),
            start: None,
        };
        let errors = update_time_line(input, &mut time_line_settings);
        assert_eq!(errors.errors.len(), 1);
        assert_eq!(errors.errors[0].field, "step");
        assert_eq!(
            errors.errors[0].message,
            "time line step should not exceed duration"
        );
        let mut expected_time_line = TimeLineSettings::default();
        expected_time_line
            .set_duration(Duration::try_new(13, 0, 0).unwrap())
            .unwrap();
        assert_eq!(time_line_settings, expected_time_line);
    }

    #[test]
    fn setting_custom_start_time_updates_start() {
        let mut time_line_settings = TimeLineSettings::default();
        let future_timestamp = "2500-01-01T00:00:00Z".to_string();
        let input = TimeLineUpdate {
            duration: None,
            step: None,
            start: Some(TimeLineStartInput {
                // Provide no clock choice; we are setting a custom start time instead.
                clock_choice: None,
                custom_start_time: Some(future_timestamp.clone()),
            }),
        };
        let errors = update_time_line(input, &mut time_line_settings);

        assert!(errors.errors.is_empty());

        match time_line_settings.start() {
            TimeLineStart::CustomStartTime(custom) => {

                let expected_dt = DateTime::parse_from_rfc3339(&future_timestamp)
                    .unwrap()
                    .with_timezone(&Utc);
                assert_eq!(custom.start_time, expected_dt);
            }
            _ => panic!("Expected CustomStartTime variant"),
        }
    }
        #[test]
    fn setting_clock_choice_updates_start() {
        let mut time_line_settings = TimeLineSettings::default();

        let input = TimeLineUpdate {
            duration: None,
            step: None,
            start: Some(TimeLineStartInput {
                clock_choice: Some(Clock::CurrentHour),
                custom_start_time: None,
            }),
        };
        let errors = update_time_line(input, &mut time_line_settings);
        assert!(errors.errors.is_empty());
        // After update, the timeline start should be a ClockChoice variant.
        match time_line_settings.start() {
            TimeLineStart::ClockChoice(clock_choice) => {
                assert_eq!(clock_choice.choice, Clock::CurrentHour);
            }
            _ => panic!("Expected ClockChoice variant"),
        }
    }
}
