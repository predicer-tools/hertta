use juniper::GraphQLObject;

#[derive(Clone, GraphQLObject)]
pub struct Status {
    state: String,
    job_id: Option<i32>,
    message: Option<String>,
}

impl Status {
    pub fn new_idle() -> Self {
        Status {
            state: "idle".to_string(),
            job_id: None,
            message: None,
        }
    }

    pub fn new_in_progress(job_id: i32) -> Self {
        Status {
            state: "in_progress".to_string(),
            job_id: Some(job_id),
            message: None,
        }
    }

    pub fn new_error(job_id: i32, message: &str) -> Self {
        Status {
            state: "error".to_string(),
            job_id: Some(job_id),
            message: Some(String::from(message)),
        }
    }

    pub fn new_finished(job_id: i32) -> Self {
        Status {
            state: "finished".to_string(),
            job_id: Some(job_id),
            message: None,
        }
    }
}
