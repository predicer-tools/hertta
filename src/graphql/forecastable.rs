use crate::input_data_base::BaseForecastable;

pub fn to_forecastable(constant: Option<f64>) -> Option<BaseForecastable> {
    constant.map(|value| BaseForecastable::Constant(value.into()))
}
