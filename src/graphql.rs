use crate::settings::{LocationSettings, Settings};
use juniper::{
    graphql_object, Context, EmptySubscription, FieldResult, GraphQLInputObject, GraphQLObject,
    RootNode,
};
use std::sync::{Arc, Mutex};

#[derive(GraphQLObject)]
#[graphql(description = "Predicer settings.")]
struct Predicer {
    #[graphql(description = "Internal network port for communication with Predicer.")]
    port: i32,
}

#[derive(GraphQLObject)]
#[graphql(description = "Optimization settings.")]
struct Optimization {
    #[graphql(description = "Time line duration in milliseconds.")]
    duration: i32,
    #[graphql(description = "Time step in milliseconds.")]
    step: i32,
}

#[derive(GraphQLObject)]
#[graphql(description = "Location settings.")]
struct Location {
    #[graphql(description = "Country name.")]
    country: String,
    #[graphql(description = "Place name.")]
    place: String,
}

#[derive(GraphQLInputObject)]
#[graphql(description = "Location settings.")]
struct LocationInput {
    #[graphql(description = "Country name.")]
    country: String,
    #[graphql(description = "Place name.")]
    place: String,
}

pub struct HerttaContext {
    settings: Arc<Mutex<Settings>>,
}

impl Context for HerttaContext {}

impl HerttaContext {
    pub fn new(settings: &Arc<Mutex<Settings>>) -> Self {
        HerttaContext {
            settings: Arc::clone(settings),
        }
    }
}

pub struct Query;

#[graphql_object]
#[graphql(context = HerttaContext)]
impl Query {
    fn api_version() -> &'static str {
        "0.9"
    }
    fn predicer(context: &HerttaContext) -> FieldResult<Predicer> {
        Ok(Predicer {
            port: context.settings.lock().unwrap().predicer_port as i32,
        })
    }
    fn optimization(context: &HerttaContext) -> FieldResult<Optimization> {
        let settings = context.settings.lock().unwrap();
        Ok(Optimization {
            duration: settings.time_line.duration.num_milliseconds() as i32,
            step: settings.time_line.step.num_milliseconds() as i32,
        })
    }
    fn location(context: &HerttaContext) -> FieldResult<Option<Location>> {
        let settings = context.settings.lock().unwrap();
        match &settings.location {
            Some(location) => Ok(Some(Location {
                country: location.country.clone(),
                place: location.place.clone(),
            })),
            None => Ok(None),
        }
    }
}

pub struct Mutation;

#[graphql_object]
#[graphql(context = HerttaContext)]
impl Mutation {
    fn set_location(new_location: LocationInput, context: &HerttaContext) -> FieldResult<Location> {
        let mut settings = context.settings.lock().unwrap();
        let location = LocationSettings {
            country: new_location.country.clone(),
            place: new_location.place.clone(),
        };
        settings.location = Some(location);
        Ok(Location {
            country: new_location.country,
            place: new_location.place,
        })
    }
}

pub type Schema = RootNode<'static, Query, Mutation, EmptySubscription<HerttaContext>>;
