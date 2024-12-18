use super::{ValidationError, ValidationErrors};
use crate::time_line_settings::{Duration, TimeLineSettings};
use juniper::GraphQLInputObject;

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
}
