use super::delete;
use super::forecastable;
use super::{MaybeError, ValidationError, ValidationErrors};
use crate::input_data::Forecast;
use crate::input_data_base::{
    BaseForecastable, BaseMarket, BaseNode, MarketDirection, MarketType, ProcessGroup, ValueInput, Value, ForecastValueInput, ForecastValue,
};
use crate::scenarios::Scenario;
use juniper::GraphQLInputObject;

#[derive(GraphQLInputObject, Debug)]
pub struct NewMarket {
    name: String,
    m_type: MarketType,
    node: String,
    process_group: String,
    direction: Option<MarketDirection>,
    realisation: Vec<ValueInput>,
    reserve_type: Option<String>,
    is_bid: bool,
    is_limited: bool,
    min_bid: f64,
    max_bid: f64,
    fee: f64,
    price: Vec<ForecastValueInput>,
    up_price: Vec<ForecastValueInput>,
    down_price: Vec<ForecastValueInput>,
    reserve_activation_price: Vec<ValueInput>,
}

impl NewMarket {
    fn to_market(self, scenarios: &Vec<Scenario>) -> BaseMarket {
        BaseMarket {
            name: self.name,
            m_type: self.m_type,
            node: self.node,
            process_group: self.process_group,
            direction: self.direction,
            realisation: self
            .realisation
            .into_iter()
            .map(Value::try_from)
            .collect::<Result<Vec<Value>, _>>()
            .expect("Could not parse reserve activation price values"),
            reserve_type: self.reserve_type,
            is_bid: self.is_bid,
            is_limited: self.is_limited,
            min_bid: self.min_bid,
            max_bid: self.max_bid,
            fee: self.fee,
            price: forecastable::convert_forecast_value_inputs(self.price, scenarios),
            up_price: forecastable::convert_forecast_value_inputs(self.up_price, scenarios),
            down_price: forecastable::convert_forecast_value_inputs(self.down_price, scenarios),
            reserve_activation_price: self
                .reserve_activation_price
                .into_iter()
                .map(Value::try_from)
                .collect::<Result<Vec<Value>, _>>()
                .expect("Could not parse reserve activation price values"),
            fixed: Vec::new(),
        }
    }
}

pub fn create_market(
    market: NewMarket,
    markets: &mut Vec<BaseMarket>,
    nodes: &Vec<BaseNode>,
    groups: &Vec<ProcessGroup>,
    scenarios: &Vec<Scenario>,
) -> ValidationErrors {
    let errors = validate_market_creation(&market, nodes, groups);
    if !errors.is_empty() {
        return ValidationErrors::from(errors);
    }
    markets.push(market.to_market(scenarios));
    ValidationErrors::default()
}

fn validate_market_creation(
    market: &NewMarket,
    nodes: &Vec<BaseNode>,
    groups: &Vec<ProcessGroup>,
) -> Vec<ValidationError> {
    let mut errors = Vec::new();
    if market.name.is_empty() {
        errors.push(ValidationError::new("name", "name is empty"));
    }
    if !nodes.iter().any(|n| n.name == market.node) {
        errors.push(ValidationError::new("node", "no such node"));
    }
    if !groups.iter().any(|g| g.name == market.process_group) {
        errors.push(ValidationError::new("processgroup", "no such group"));
    }
    if let Some(ref reserve_type) = market.reserve_type {
        if reserve_type.is_empty() {
            errors.push(ValidationError::new(
                "reserve_type",
                "reserve_type is empty",
            ));
        }
    }
    if market.min_bid > market.max_bid {
        errors.push(ValidationError::new("min_bid", "greater than max_bid"));
    }
    errors
}

pub fn connect_market_prices_to_forecast(
    market_name: &str,
    forecast_name: String,
    forecast_type: String,
    markets: &mut Vec<BaseMarket>,
) -> MaybeError {
    let market = match markets.iter_mut().find(|m| m.name == market_name) {
        Some(market) => market,
        None => return "no such market".into(),
    };
    market.price = vec![ForecastValue {
        scenario: None,
        value: BaseForecastable::Forecast(Forecast::new(forecast_name.clone(), forecast_type.clone())),
    }];
    market.up_price = vec![ForecastValue {
        scenario: None,
        value: BaseForecastable::Forecast(Forecast::new(forecast_name.clone(),forecast_type.clone())),
    }];
    market.down_price = vec![ForecastValue {
        scenario: None,
        value: BaseForecastable::Forecast(Forecast::new(forecast_name.clone(),forecast_type.clone())),
    }];
    MaybeError::new_ok()
}

pub fn delete_market(name: &str, markets: &mut Vec<BaseMarket>) -> MaybeError {
    delete::delete_named(name, markets)
}
