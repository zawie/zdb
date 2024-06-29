use std::collections::BTreeMap;

use super::{Storage, SetResult, GetResult};
pub struct MemoryStore {
    map: BTreeMap<String, String>,
    memory_usage: usize
}

impl Storage for MemoryStore {
    fn set(&mut self, key: &str, value: &str) -> SetResult {
        let key_len = key.len();
        let value_len = value.len();
        match self.map.insert(key.to_owned(), value.to_owned()) {
            Some(v) => {
                self.memory_usage += value_len;
                self.memory_usage -= v.len();
            }
            None => {
                self.memory_usage += value_len + key_len;
            }
        }
        return Ok(())
    }

    fn get(&self, key: &str) -> GetResult {
        match self.map.get(key) {
            Some(value) => Ok(Some(value.to_owned())),
            None => Ok(None)
        }
    }
} 

impl MemoryStore {
    pub fn new() -> MemoryStore {
        MemoryStore {
            map: BTreeMap::new(),
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
        
        match store.set(k, v) {
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

        let k = "key";
        let v = "value";

        let expected_memory_usage = k.len() + v.len();

        assert!(store.memory_usage == 0, "Memory usage should start empty.");
        assert!(store.set(k, v).is_ok(), "Set failed");
        assert!(store.memory_usage == expected_memory_usage, "Memory usage after first set is not correct! expected: {} got: {}", expected_memory_usage, store.memory_usage);


        let v = "a different value ";

        let expected_memory_usage = k.len() + v.len();

        assert!(store.set(k, v).is_ok(), "Set failed");
        assert!(store.memory_usage == expected_memory_usage, "Memory usage after second set is not correct! expected: {} got: {}", expected_memory_usage, store.memory_usage);
    }

}