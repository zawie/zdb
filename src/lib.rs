pub mod database;
mod memory_store;
mod log_store;
mod segment_store;

use std::error::Error;

type SetResult = Result<(), Box<dyn Error>>;
type GetResult = Result<Option<String>, Box<dyn Error>>;
pub trait Storage {
    fn set(&mut self, key: &str, value: &str) -> SetResult;
    fn get(&self, key: &str) -> GetResult;
}