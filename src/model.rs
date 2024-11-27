use crate::input_data_base::BaseInputData;
use crate::time_line_settings::TimeLineSettings;
use juniper::GraphQLObject;
use serde::{Deserialize, Serialize};

#[derive(Clone, Default, Deserialize, GraphQLObject, Serialize)]
#[graphql(description = "Optimization model.")]
pub struct Model {
    #[serde(default)]
    pub time_line: TimeLineSettings,
    pub input_data: BaseInputData,
}
