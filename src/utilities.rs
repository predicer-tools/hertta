use crate::input_data;

use std::sync::{Arc};
use tokio::sync::Mutex;
use arrow::array::{Array, StringArray, Float64Array, Int32Array, ArrayRef};


//POISTETTAVA

pub async fn update_all_ts_data(optimization_data: Arc<Mutex<input_data::OptimizationData>>) {
    let mut data = optimization_data.lock().await;
    if let Some(ref mut model_data) = data.model_data {
        let temporals_t = &model_data.temporals.t;
        let default_ts_data = input_data::TimeSeriesData::from_temporals(temporals_t, "default_scenario".to_string());
        println!("Default TS Data: {:?}", default_ts_data);

        for process in model_data.processes.values_mut() {
            process.cf = default_ts_data.clone();
            process.eff_ts = default_ts_data.clone();
            for topo in process.topos.iter_mut() {
                topo.cap_ts = default_ts_data.clone();
            }
            println!("Updated Process: {:?}", process);
        }

        for node in model_data.nodes.values_mut() {
            node.cost = default_ts_data.clone();
            node.inflow = default_ts_data.clone();
            println!("Updated Node: {:?}", node);
        }

        for node_diffusion in model_data.node_diffusion.iter_mut() {
            node_diffusion.coefficient = default_ts_data.clone();
            println!("Updated Node Diffusion: {:?}", node_diffusion);
        }

        for node_history in model_data.node_histories.values_mut() {
            node_history.steps = default_ts_data.clone();
            println!("Updated Node History: {:?}", node_history);
        }

        for market in model_data.markets.values_mut() {
            market.realisation = default_ts_data.clone();
            market.price = default_ts_data.clone();
            market.up_price = default_ts_data.clone();
            market.down_price = default_ts_data.clone();
            market.reserve_activation_price = default_ts_data.clone();
            println!("Updated Market: {:?}", market);
        }

        for inflow_block in model_data.inflow_blocks.values_mut() {
            inflow_block.data = default_ts_data.clone();
            println!("Updated Inflow Block: {:?}", inflow_block);
        }

        for bid_slot in model_data.bid_slots.values_mut() {
            bid_slot.time_steps = temporals_t.clone();
            println!("Updated Bid Slot: {:?}", bid_slot);
        }

        for gen_constraint in model_data.gen_constraints.values_mut() {
            gen_constraint.constant = default_ts_data.clone();
            for factor in gen_constraint.factors.iter_mut() {
                factor.data = default_ts_data.clone();
            }
            println!("Updated Gen Constraint: {:?}", gen_constraint);
        }
    }
}


//PIDETÄÄN

pub fn column_value_to_string(column: &ArrayRef, row_index: usize) -> String {
    if column.is_null(row_index) {
        return "NULL".to_string();
    }

    match column.data_type() {
        arrow::datatypes::DataType::Utf8 => {
            let array = column.as_any().downcast_ref::<StringArray>().unwrap();
            array.value(row_index).to_string()
        }
        arrow::datatypes::DataType::Float64 => {
            let array = column.as_any().downcast_ref::<Float64Array>().unwrap();
            array.value(row_index).to_string()
        }
        arrow::datatypes::DataType::Int32 => {
            let array = column.as_any().downcast_ref::<Int32Array>().unwrap();
            array.value(row_index).to_string()
        }
        _ => "Unsupported type".to_string(),
    }
}

/// Prints each element of a generic vector to the console.
///
/// This function is generic over the element type, requiring that each element implement
/// the `std::fmt::Display` trait for formatting.
///
/// # Arguments
///
/// - `vec`: A reference to the vector containing elements of type `T`.
///
pub fn _print_vector<T: std::fmt::Display>(vec: &Vec<T>) {
    for item in vec.iter() {
        println!("{}", item);
    }
}

pub fn check_series(ts_data: &input_data::TimeSeries, temporals_t: &[String], context: &str) {
    let series_keys: Vec<String> = ts_data.series.keys().cloned().collect();
    if series_keys != *temporals_t {
        println!("Mismatch in {}: {:?}", context, ts_data.scenario);
        println!("Expected: {:?}", temporals_t);
        println!("Found: {:?}", series_keys);
    }
}

pub async fn check_ts_data_against_temporals(optimization_data: Arc<Mutex<input_data::OptimizationData>>) {
    let data = optimization_data.lock().await;
    if let Some(model_data) = &data.model_data {
        let temporals_t = &model_data.temporals.t;

        for (process_name, process) in &model_data.processes {
            for ts_data in &process.cf.ts_data {
                check_series(&ts_data, temporals_t, process_name);
            }
            for ts_data in &process.eff_ts.ts_data {
                check_series(&ts_data, temporals_t, process_name);
            }
            for topology in &process.topos {
                for ts_data in &topology.cap_ts.ts_data {
                    check_series(&ts_data, temporals_t, process_name);
                }
            }
        }

        for (node_name, node) in &model_data.nodes {
            for ts_data in &node.cost.ts_data {
                check_series(&ts_data, temporals_t, node_name);
            }
            for ts_data in &node.inflow.ts_data {
                check_series(&ts_data, temporals_t, node_name);
            }
        }

        for node_diffusion in &model_data.node_diffusion {
            for ts_data in &node_diffusion.coefficient.ts_data {
                check_series(&ts_data, temporals_t, &format!("diffusion {}-{}", node_diffusion.node1, node_diffusion.node2));
            }
        }

        for (market_name, market) in &model_data.markets {
            for ts_data in &market.realisation.ts_data {
                check_series(&ts_data, temporals_t, market_name);
            }
            for ts_data in &market.price.ts_data {
                check_series(&ts_data, temporals_t, market_name);
            }
            for ts_data in &market.up_price.ts_data {
                check_series(&ts_data, temporals_t, market_name);
            }
            for ts_data in &market.down_price.ts_data {
                check_series(&ts_data, temporals_t, market_name);
            }
            for ts_data in &market.reserve_activation_price.ts_data {
                check_series(&ts_data, temporals_t, market_name);
            }
        }
    }
}

/// Prints each element of a vector of tuples `(String, f64)` to the console.
///
/// Specifically formatted to handle a vector of tuples, where each tuple consists of
/// a `String` and an `f64`, printing them in a `name: value` format.
///
/// # Arguments
///
/// - `v`: A reference to the vector containing tuples of `(String, f64)`.
///
pub fn _print_tuple_vector(v: &Vec<(String, f64)>) {
    for (name, value) in v {
        println!("{}: {}", name, value);
    }
}

/// Combines two vectors into a single vector of tuples.
///
/// The function checks if both input vectors are of the same length and returns an error if they are not.
///
/// # Arguments
///
/// - `solution_vector`: A mutable reference to the vector that will store the combined tuples.
///   This vector is cleared at the beginning of the function to ensure it only contains the 
///   combined results.
/// - `vector1`: A vector of `String` elements, each representing the first element of the tuple.
/// - `vector2`: A vector of `f64` elements, each representing the second element of the tuple.
///
/// # Errors
///
/// Returns an `Err` with a string message if `vector1` and `vector2` have different lengths.
///
pub fn _combine_vectors(vector1: Vec<String>, vector2: Vec<f64>) -> Result<Vec<(String, f64)>, String> {
    if vector1.len() != vector2.len() {
        return Err("Vectors have different lengths".to_string());
    }

    let combined_vector: Vec<(String, f64)> = vector1.into_iter().zip(vector2.into_iter()).collect();

    Ok(combined_vector)
}


/// Checks if a string is a valid HTTP header value.
///
/// This function validates a given string to see if it is suitable to be used as an HTTP header value.
/// The criteria for this example includes being an ASCII string and not containing any control characters.
///
/// # Arguments
///
/// - `value`: A string slice reference that is to be validated.
///
/// # Returns
///
/// Returns `true` if the string is ASCII and does not contain control characters, otherwise `false`.
///
pub fn _is_valid_http_header_value(value: &str) -> bool {
    value.is_ascii() && !value.chars().any(|ch| ch.is_control())
}

#[cfg(test)]
mod tests {
    use super::*;

    /* 
    #[test]
    fn test_combine_vectors_success() {
        let vector1 = vec!["a".to_string(), "b".to_string()];
        let vector2 = vec![1.0, 2.0];

        assert!(combine_vectors(vector1, vector2).is_ok());
        assert_eq!(solution_vector, vec![("a".to_string(), 1.0), ("b".to_string(), 2.0)]);
    }
    

    #[test]
    fn test_combine_vectors_different_length() {
        let vector1 = vec!["a".to_string()];
        let vector2 = vec![1.0, 2.0];

        assert!(combine_vectors(vector1, vector2).is_err());
    }

    #[test]
    fn test_combine_vectors_empty() {
        let mut solution_vector = Vec::new();
        let vector1: Vec<String> = Vec::new();
        let vector2: Vec<f64> = Vec::new();

        assert!(combine_vectors(&mut solution_vector, vector1, vector2).is_ok());
        assert!(solution_vector.is_empty());
    }

    */
}


