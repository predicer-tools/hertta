use super::{ValidationError, ValidationErrors};
use crate::input_data::Group;
use crate::input_data_base::{BaseMarket, BaseNode, BaseProcess, GroupMember};
use juniper::GraphQLInputObject;

#[derive(GraphQLInputObject)]
pub struct AddMarketInput {
    name: String,
    m_type: String,
    node: String,
    processgroup: String,
    direction: String,
    realisation: f64,
    reserve_type: String,
    is_bid: bool,
    is_limited: bool,
    min_bid: f64,
    max_bid: f64,
    fee: f64,
    price: f64,
    up_price: f64,
    down_price: f64,
    reserve_activation_price: f64,
}

impl AddMarketInput {
    fn to_market(self) -> BaseMarket {
        BaseMarket {
            name: self.name,
            m_type: self.m_type,
            node: self.node,
            processgroup: self.processgroup,
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

pub fn add_market(
    market: AddMarketInput,
    markets: &mut Vec<BaseMarket>,
    nodes: &Vec<BaseNode>,
    groups: &Vec<Group>,
) -> ValidationErrors {
    let errors = validate_market_to_add(&market, nodes, groups);
    if !errors.is_empty() {
        return ValidationErrors::from(errors);
    }
    markets.push(market.to_market());
    ValidationErrors::default()
}

fn validate_market_to_add(
    market: &AddMarketInput,
    nodes: &Vec<BaseNode>,
    groups: &Vec<Group>,
) -> Vec<ValidationError> {
    let mut errors = Vec::new();
    if market.name.is_empty() {
        errors.push(ValidationError::new("name", "name is empty"));
    }
    if ["energy", "reserve"]
        .iter()
        .find(|t| **t == market.m_type)
        .is_none()
    {
        errors.push(ValidationError::new(
            "m_type",
            "should be 'energy' or 'reserve'",
        ));
    }
    if nodes.iter().find(|n| n.name == market.node).is_none() {
        errors.push(ValidationError::new("node", "no such node"));
    }
    if let Some(group) = groups.iter().find(|g| g.name == market.processgroup) {
        if group.g_type != BaseProcess::group_type() {
            errors.push(ValidationError::new("processgroup", "wrong group type"));
        }
    } else {
        errors.push(ValidationError::new("processgroup", "no such group"));
    }
    if ["up", "down", "updown"]
        .iter()
        .find(|d| **d == market.direction)
        .is_none()
    {
        errors.push(ValidationError::new(
            "direction",
            "should be 'up', 'down' or 'updown'",
        ));
    }
    if market.realisation < 0.0 || market.realisation > 1.0 {
        errors.push(ValidationError::new("realisation", "should be in [0, 1]"));
    }
    if market.reserve_type.is_empty() {
        errors.push(ValidationError::new(
            "reserve_type",
            "reserve_type is empty",
        ));
    }
    if market.min_bid > market.max_bid {
        errors.push(ValidationError::new("min_bid", "greater than max_bid"));
    }
    errors
}
