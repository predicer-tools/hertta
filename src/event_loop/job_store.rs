use super::jobs::JobStatus;
use std::collections::BTreeMap;
use std::sync::Arc;
use tokio::sync::RwLock;

#[derive(Clone)]
pub struct JobStore {
    finished_job_capacity: Arc<RwLock<i32>>,
    store: Arc<RwLock<BTreeMap<i32, Arc<JobStatus>>>>,
    next_job_id: Arc<RwLock<i32>>,
}

impl Default for JobStore {
    fn default() -> Self {
        JobStore {
            finished_job_capacity: Arc::new(RwLock::new(23)),
            store: Arc::new(RwLock::new(BTreeMap::new())),
            next_job_id: Arc::new(RwLock::new(1)),
        }
    }
}

impl JobStore {
    async fn next_job_id(&self) -> i32 {
        let mut next_job_id = self.next_job_id.write().await;
        let job_id: i32 = *next_job_id;
        *next_job_id += 1;
        job_id
    }

    pub async fn create_queued_job(&self) -> i32 {
        let job_id = self.next_job_id().await;
        let mut store = self.store.write().await;
        store.insert(job_id, Arc::new(JobStatus::Queued));
        Self::cull_finished(&mut store, *self.finished_job_capacity.read().await);
        job_id
    }
    fn cull_finished(store: &mut BTreeMap<i32, Arc<JobStatus>>, capacity: i32) {
        let finished_job_ids: Vec<i32> = store
            .iter()
            .filter(|(_, s)| match ***s {
                JobStatus::Queued | JobStatus::InProgress => true,
                _ => false,
            })
            .map(|(k, _)| *k)
            .collect();
        let difference = finished_job_ids.len() as i32 - capacity;
        if difference > 0 {
            for i in 0..(difference as usize) {
                store.remove(&finished_job_ids[i]);
            }
        }
    }
    pub async fn job_status(&self, job_id: i32) -> Option<Arc<JobStatus>> {
        self.store.read().await.get(&job_id).cloned()
    }
    pub async fn set_job_status(&self, job_id: i32, status: Arc<JobStatus>) -> Result<(), String> {
        let mut store = self.store.write().await;
        if !store.contains_key(&job_id) {
            return Err("no such job".into());
        }
        store.insert(job_id, status);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[tokio::test]
    async fn set_job_status_via_clone() {
        let job_store = JobStore::default();
        let clone = job_store.clone();
        let job_id = clone.create_queued_job().await;
        let status = job_store.job_status(job_id).await.unwrap();
        match *status {
            JobStatus::Queued => (),
            _ => panic!("job status should be Queued"),
        }
        clone
            .set_job_status(job_id, Arc::new(JobStatus::InProgress))
            .await
            .expect("setting job status should succeed");
        let status = job_store.job_status(job_id).await.unwrap();
        match *status {
            JobStatus::InProgress => (),
            _ => panic!("job status should be InProgress"),
        }
    }
}
