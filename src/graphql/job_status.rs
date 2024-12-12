use juniper::{GraphQLEnum, GraphQLObject};

#[derive(Clone, Copy, GraphQLEnum)]
pub enum JobState {
    Queued,
    InProgress,
    Failed,
    Finished,
}

#[derive(Clone, GraphQLObject)]
pub struct JobStatus {
    state: JobState,
    message: Option<String>,
}

impl JobStatus {
    pub fn new_queued() -> Self {
        JobStatus {
            state: JobState::Queued,
            message: None,
        }
    }

    pub fn new_in_progress() -> Self {
        JobStatus {
            state: JobState::InProgress,
            message: None,
        }
    }

    pub fn new_failed(message: String) -> Self {
        JobStatus {
            state: JobState::Failed,
            message: Some(message),
        }
    }

    pub fn new_finished() -> Self {
        JobStatus {
            state: JobState::Finished,
            message: None,
        }
    }
}
