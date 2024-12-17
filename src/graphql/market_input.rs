use super::delete;
use super::{MaybeError, ValidationError, ValidationErrors};
use crate::input_data_base::{BaseMarket, BaseNode, MarketDirection, MarketType, ProcessGroup};
use juniper::GraphQLInputObject;

#[derive(GraphQLInputObject)]
pub struct NewMarket {
    name: String,
    m_type: MarketType,
    node: String,
    process_group: String,
    direction: Option<MarketDirection>,
    realisation: Option<f64>,
    reserve_type: Option<String>,
    is_bid: bool,
    is_limited: bool,
    min_bid: f64,
    max_bid: f64,
    fee: f64,
    price: Option<f64>,
    up_price: Option<f64>,
    down_price: Option<f64>,
    reserve_activation_price: Option<f64>,
}

impl NewMarket {
    fn to_market(self) -> BaseMarket {
        BaseMarket {
            name: self.name,
            m_type: self.m_type,
            node: self.node,
            process_group: self.process_group,
            direction: self.direction,
            realisation: self.realisation,
            reserve_type: self.reserve_type,
            is_bid: self.is_bid,
            is_limited: self.is_limited,
            min_bid: self.min_bid,
            max_bid: self.max_bid,
            fee: self.fee,
            price: self.price,
            up_price: self.up_price,
            down_price: self.down_price,
            reserve_activation_price: self.reserve_activation_price,
            fixed: Vec::new(),
        }
    }
}

pub fn create_market(
    market: NewMarket,
    markets: &mut Vec<BaseMarket>,
    nodes: &Vec<BaseNode>,
    groups: &Vec<ProcessGroup>,
) -> ValidationErrors {
    let errors = validate_market_creation(&market, nodes, groups);
    if !errors.is_empty() {
        return ValidationErrors::from(errors);
    }
    markets.push(market.to_market());
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
    if nodes.iter().find(|n| n.name == market.node).is_none() {
        errors.push(ValidationError::new("node", "no such node"));
    }
    if groups
        .iter()
        .find(|g| g.name == market.process_group)
        .is_none()
    {
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

pub fn delete_market(name: &str, markets: &mut Vec<BaseMarket>) -> MaybeError {
    delete::delete_named(name, markets)
}
