use juniper::{
    GraphQLScalar, InputValue, ParseScalarResult, ParseScalarValue, ScalarToken, ScalarValue, Value,
};
use serde::de::{self, Visitor};
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use std::fmt::{self, Formatter};

#[derive(Clone, Copy, Debug, Default, GraphQLScalar, PartialEq)]
#[graphql(to_output_with = Self::to_output, from_input_with = Self::from_input, parse_token_with = Self::parse_token, transparent)]
pub struct LongInt {
    pub value: i64,
}

impl Serialize for LongInt {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_i64(self.value)
    }
}

impl From<i64> for LongInt {
    fn from(value: i64) -> Self {
        LongInt { value: value }
    }
}

impl From<i32> for LongInt {
    fn from(value: i32) -> Self {
        LongInt {
            value: value as i64,
        }
    }
}

struct LongIntVisitor;

impl<'de> Visitor<'de> for LongIntVisitor {
    type Value = LongInt;

    fn expecting(&self, formatter: &mut Formatter) -> fmt::Result {
        formatter.write_str("an integer between -2^63 and 2^63")
    }

    fn visit_i64<E: de::Error>(self, value: i64) -> Result<Self::Value, E>
    where
        E: de::Error,
    {
        Ok(LongInt { value })
    }
    fn visit_u64<E: de::Error>(self, value: u64) -> Result<Self::Value, E>
    where
        E: de::Error,
    {
        let value = match i64::try_from(value) {
            Ok(x) => x,
            Err(..) => return Err(E::custom(format!("u64 out of range: {}", value))),
        };
        Ok(LongInt { value: value })
    }
}

impl<'de> Deserialize<'de> for LongInt {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<LongInt, D::Error> {
        deserializer.deserialize_i64(LongIntVisitor)
    }
}

impl LongInt {
    fn to_output<S: ScalarValue>(&self) -> Value<S> {
        Value::from(self.value as i32)
    }

    fn from_input<S: ScalarValue>(input: &InputValue<S>) -> Result<Self, String> {
        input
            .as_int_value()
            .ok_or_else(|| format!("Expected `Int`, found: {input}"))
            .map(|x| Self { value: x as i64 })
    }

    fn parse_token<S: ScalarValue>(value: ScalarToken<'_>) -> ParseScalarResult<S> {
        <i32 as ParseScalarValue<S>>::from_str(value)
    }
}
