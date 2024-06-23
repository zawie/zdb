use std::{collections::HashMap, error::Error};


type SetResult = std::result::Result<(), Box<dyn Error>>;
type GetResult = std::result::Result<Option<String>, Box<dyn Error>>;

pub struct Database {
    map: HashMap<String, String>
}



impl Database {
    pub fn new() -> Database {
        Database {
            map: HashMap::new()
        }
    }

    pub fn set(&mut self, key: String, value: String) -> SetResult {
        self.map.insert(key, value);

        return Ok(())
    }

    pub fn get(&self, key: &str) -> GetResult {
        
        match self.map.get(key) {
            Some(value) => Ok(Some(value.clone())),
            None => Ok(None)
        }
    }
}