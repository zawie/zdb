use crate::memory_store::MemoryStore;

use super::{Storage, SetResult, GetResult};
pub struct Database {
    memory: MemoryStore,
}

impl Database {
    pub fn new() -> Database {
        Database {
            memory: MemoryStore::new(),
        }
    }
}

impl Storage for Database {
    fn set(&mut self, key: &str, value: &str) -> SetResult {
        self.memory.set(key, value)
    }

    fn get(&self, key: &str) -> GetResult {
        self.memory.get(key)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_set_get() {
        let mut db = Database::new();

        let k = "key";
        let v = "value";
        
        match db.set(k, v) {
            Ok(_) => {
                match db.get("key") {
                    Ok(_) => {
                        match db.get("key").unwrap() {
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

}