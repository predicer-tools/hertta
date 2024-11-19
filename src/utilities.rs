use crate::input_data;
use crate::{TimeLine, TimeStamp};

use std::sync::Arc;
use tokio::sync::Mutex;

fn check_series(ts_data: &input_data::TimeSeries, temporals_t: &[TimeStamp], context: &str) {
    let series_keys: TimeLine = ts_data.series.keys().cloned().collect();
    if series_keys != *temporals_t {
        println!("Mismatch in {}: {:?}", context, ts_data.scenario);
        println!("Expected: {:?}", temporals_t);
        println!("Found: {:?}", series_keys);
    }
}

pub async fn check_ts_data_against_temporals(
    optimization_data: Arc<Mutex<input_data::OptimizationData>>,
) {
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
                check_series(
                    &ts_data,
                    temporals_t,
                    &format!(
                        "diffusion {}-{}",
                        node_diffusion.node1, node_diffusion.node2
                    ),
                );
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
