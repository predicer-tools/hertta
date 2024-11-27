use crate::{TimeLine, TimeStamp};
use chrono::TimeDelta;

pub fn make_time_data(start_time: TimeStamp, step: TimeDelta, duration: TimeDelta) -> TimeLine {
    let n_steps: i32 = (duration.num_milliseconds() / step.num_milliseconds())
        .try_into()
        .unwrap();
    let n_stamps = n_steps + 1;
    let mut series = Vec::with_capacity(n_stamps as usize);
    for i in 0..n_stamps {
        series.push(start_time + step * i);
    }
    series
}

pub fn extract_values_from_pairs_checked<T: Clone>(
    pairs: &Vec<(TimeStamp, T)>,
    true_time_data: &TimeLine,
) -> Result<Vec<T>, String> {
    if pairs.len() != true_time_data.len() {
        return Err("time series lenghts do not match".to_string());
    }
    let mut values = Vec::with_capacity(pairs.len());
    for (i, pair) in pairs.iter().enumerate() {
        if pair.0 != true_time_data[i] {
            return Err(format!(
                "time stamps do not match ({} vs. {})",
                pair.0, true_time_data[i]
            ));
        }
        values.push(pair.1.clone());
    }
    Ok(values)
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::{TimeZone, Utc};
    #[test]
    fn make_simple_time_data() {
        let start = Utc
            .with_ymd_and_hms(2024, 11, 6, 8, 43, 0)
            .single()
            .expect("UTC times should map to unique time stamps");
        let step = TimeDelta::minutes(15);
        let duration = TimeDelta::hours(4);
        let time_data = make_time_data(start.into(), step, duration);
        assert_eq!(time_data.len(), 17);
    }
    #[test]
    fn check_time_stamps() {
        let start = Utc
            .with_ymd_and_hms(2024, 11, 6, 8, 43, 0)
            .single()
            .expect("UTC times should map to unique time stamps");
        let step = TimeDelta::minutes(15);
        let duration = TimeDelta::hours(1);
        let time_data = make_time_data(start.into(), step, duration);
        assert_eq!(time_data.len(), 5);
        let expected_stamps: TimeLine = vec![
            Utc.with_ymd_and_hms(2024, 11, 6, 8, 43, 0)
                .single()
                .expect("UTC times should map to unique time stamps")
                .into(),
            Utc.with_ymd_and_hms(2024, 11, 6, 8, 58, 0)
                .single()
                .expect("UTC times should map to unique time stamps")
                .into(),
            Utc.with_ymd_and_hms(2024, 11, 6, 9, 13, 0)
                .single()
                .expect("UTC times should map to unique time stamps")
                .into(),
            Utc.with_ymd_and_hms(2024, 11, 6, 9, 28, 0)
                .single()
                .expect("UTC times should map to unique time stamps")
                .into(),
            Utc.with_ymd_and_hms(2024, 11, 6, 9, 43, 0)
                .single()
                .expect("UTC times should map to unique time stamps")
                .into(),
        ];
        assert_eq!(time_data, expected_stamps);
    }
    #[test]
    fn extract_values_with_check_succeeds() {
        let pairs: Vec<(TimeStamp, f64)> = vec![
            (
                Utc.with_ymd_and_hms(2024, 11, 6, 8, 43, 0)
                    .single()
                    .unwrap()
                    .into(),
                -1.1,
            ),
            (
                Utc.with_ymd_and_hms(2024, 11, 6, 8, 58, 0)
                    .single()
                    .unwrap()
                    .into(),
                -1.2,
            ),
            (
                Utc.with_ymd_and_hms(2024, 11, 6, 9, 13, 0)
                    .single()
                    .unwrap()
                    .into(),
                -1.3,
            ),
        ];
        let time_stamps: TimeLine = vec![
            Utc.with_ymd_and_hms(2024, 11, 6, 8, 43, 0)
                .single()
                .unwrap()
                .into(),
            Utc.with_ymd_and_hms(2024, 11, 6, 8, 58, 0)
                .single()
                .unwrap()
                .into(),
            Utc.with_ymd_and_hms(2024, 11, 6, 9, 13, 0)
                .single()
                .unwrap()
                .into(),
        ];
        let values = extract_values_from_pairs_checked(&pairs, &time_stamps)
            .expect("extracting should not fail");
        let expected = [-1.1, -1.2, -1.3];
        assert_eq!(values.len(), expected.len());
        for (i, x) in values.iter().enumerate() {
            assert_eq!(*x, expected[i]);
        }
    }
    #[test]
    fn extract_values_fails_when_time_series_length_mismatches() {
        let pairs: Vec<(TimeStamp, f64)> = vec![
            (
                Utc.with_ymd_and_hms(2024, 11, 6, 8, 43, 0)
                    .single()
                    .unwrap()
                    .into(),
                -1.1,
            ),
            (
                Utc.with_ymd_and_hms(2024, 11, 6, 8, 58, 0)
                    .single()
                    .unwrap()
                    .into(),
                -1.2,
            ),
            (
                Utc.with_ymd_and_hms(2024, 11, 6, 9, 13, 0)
                    .single()
                    .unwrap()
                    .into(),
                -1.3,
            ),
        ];
        let time_stamps: TimeLine = Vec::new();
        if let Ok(_) = extract_values_from_pairs_checked(&pairs, &time_stamps) {
            panic!("expected call to return error");
        }
    }
    #[test]
    fn extract_values_fails_when_time_stamps_mismatch() {
        let pairs: Vec<(TimeStamp, f64)> = vec![
            (
                Utc.with_ymd_and_hms(2024, 11, 6, 8, 43, 0)
                    .single()
                    .unwrap()
                    .into(),
                -1.1,
            ),
            (
                Utc.with_ymd_and_hms(2024, 11, 6, 8, 58, 0)
                    .single()
                    .unwrap()
                    .into(),
                -1.2,
            ),
            (
                Utc.with_ymd_and_hms(2024, 11, 6, 9, 13, 0)
                    .single()
                    .unwrap()
                    .into(),
                -1.3,
            ),
        ];
        let time_stamps: TimeLine = vec![
            Utc.with_ymd_and_hms(2024, 11, 6, 8, 43, 0)
                .single()
                .unwrap()
                .into(),
            Utc.with_ymd_and_hms(2025, 11, 6, 8, 58, 0)
                .single()
                .unwrap()
                .into(),
            Utc.with_ymd_and_hms(2024, 11, 6, 9, 13, 0)
                .single()
                .unwrap()
                .into(),
        ];
        if let Ok(_) = extract_values_from_pairs_checked(&pairs, &time_stamps) {
            panic!("expected call to return error");
        }
    }
}
