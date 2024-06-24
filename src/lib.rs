pub mod database;
pub mod memory_store;

use std::error::Error;

type SetResult = Result<(), Box<dyn Error>>;
type GetResult = Result<Option<String>, Box<dyn Error>>;
pub trait Storage {
    fn set(&mut self, key: String, value: String) -> SetResult;
    fn get(&self, key: &str) -> GetResult;
}