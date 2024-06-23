use std::{collections::HashMap, error::Error};


type SetResult = std::result::Result<(), Box<dyn Error>>;
type GetResult = std::result::Result<Option<String>, Box<dyn Error>>;

pub struct Database {
    map: HashMap<String, String>,
    memory_usage: usize
}



impl Database {
    pub fn new() -> Database {
        Database {
            map: HashMap::new(),
            memory_usage: 0,
        }
    }

    pub fn set(&mut self, key: String, value: String) -> SetResult {
        self.memory_usage += key.len() + value.len();
        match self.map.insert(key, value) {
            Some(v) => {
                self.memory_usage -= v.len();
            }
            None => {}
        }
        return Ok(())
    }

    pub fn get(&self, key: &str) -> GetResult {
        match self.map.get(key) {
            Some(value) => Ok(Some(value.clone())),
            None => Ok(None)
        }
    }
}