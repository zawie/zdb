use std::collections::HashMap;

use super::{Storage, SetResult, GetResult};
pub struct MemoryStore {
    map: HashMap<String, String>,
    memory_usage: usize
}

impl Storage for MemoryStore {
    fn set(&mut self, key: String, value: String) -> SetResult {
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

    fn get(&self, key: &str) -> GetResult {
        match self.map.get(key) {
            Some(value) => Ok(Some(value.clone())),
            None => Ok(None)
        }
    }
} 

impl MemoryStore {
    pub fn new() -> MemoryStore {
        MemoryStore {
            map: HashMap::new(),
            memory_usage: 0
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_set_get() {
        let mut store = MemoryStore::new();

        let k = "key";
        let v = "value";
        
        match store.set(k.to_string(), v.to_string()) {
            Ok(_) => {
                match store.get("key") {
                    Ok(_) => {
                        match store.get("key").unwrap() {
                            Some(value) => {
                                assert_eq!(value, v);
                            }
                            None => {
                                assert!(false, "Key not found after set");
                            }
                        }
                    }
                    Err(e) => {
                        assert!(false, "Get failed: {}", e);
                    }
                
                }
            }
            Err(e) => {
                assert!(false, "Set failed: {}", e);
            }
        }

        
    }

    #[test]
    fn test_memory_usage() {

        let mut store = MemoryStore::new();

        let k = "key".to_string();
        let v = "value".to_string();

        let expected_memory_usage = k.len() + v.len();

        assert!(store.memory_usage == 0, "Memory usage should start empty.");
        assert!(store.set(k, v).is_ok(), "Set failed");
        assert!(store.memory_usage == expected_memory_usage, "Memory usage after first set is not correct! expected: {} got: {}", expected_memory_usage, store.memory_usage);


        let k = "key".to_string();
        let v = "a different value ".to_string();

        let expected_memory_usage = k.len() + v.len();

        assert!(store.set(k, v).is_ok(), "Set failed");
        assert!(store.memory_usage == expected_memory_usage, "Memory usage after second set is not correct! expected: {} got: {}", expected_memory_usage, store.memory_usage);
    }

}