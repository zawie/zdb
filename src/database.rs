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
        let key_len = key.len();
        let value_len: usize = value.len();
        match self.map.insert(key, value) {
            Some(v) => {
                self.memory_usage += value_len - v.len();
            }
            None => {
                self.memory_usage += value_len + key_len;
            }
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_memory_usage() {

        let mut db = Database::new();

        let k = "key".to_string();
        let v = "value".to_string();

        let expected_memory_usage = k.len() + v.len();

        assert!(db.memory_usage == 0, "Memory usage should start empty.");
        assert!(db.set(k, v).is_ok(), "Set failed");
        assert!(db.memory_usage == expected_memory_usage, "Memory usage after first set is not correct! expected: {} got: {}", expected_memory_usage, db.memory_usage);


        let k = "key".to_string();
        let v = "a different value ".to_string();

        let expected_memory_usage = k.len() + v.len();

        assert!(db.set(k, v).is_ok(), "Set failed");
        assert!(db.memory_usage == expected_memory_usage, "Memory usage after second set is not correct! expected: {} got: {}", expected_memory_usage, db.memory_usage);
    }



}