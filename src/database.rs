use crate::{log_store::LogStore, memory_store::MemoryStore};

use super::{Storage, SetResult, GetResult};
pub struct Database {
    memory: MemoryStore,
    log: LogStore
}

impl Database {
    pub fn new() -> Result<Database, Box<dyn std::error::Error>> {
        let mut db = Database {
            memory: MemoryStore::new(),
            log: LogStore::init("temp.log".to_string())
        };

        let entries = match db.log.iter() {
            Ok(entries) => entries,
            Err(e) => return Err(e.into())
        };
        
        for (k,v) in entries {
            match db.memory.set(&k, &v) {
                Err(e) => return Err(e),
                _ => {}
            }
        }

        Ok(db)
    }
}

impl Storage for Database {
    fn set(&mut self, key: &str, value: &str) -> SetResult {
        self.log.set(key, value)?;
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
        let mut db = Database::new().expect("Failed to create database");

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