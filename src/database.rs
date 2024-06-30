use crate::{log_store::LogStore, memory_store::MemoryStore, segment_store::SegmentStore};

use super::{Storage, SetResult, GetResult};
pub struct Database {
    memory: MemoryStore,
    log: LogStore,
    segments: Vec<SegmentStore>,
}

impl Database {
    pub fn new() -> Result<Database, Box<dyn std::error::Error>> {
        let mut db = Database {
            memory: MemoryStore::new(),
            log: LogStore::init("temp.log".to_string()),
            segments: Vec::new(),
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
        self.memory.set(key, value)?;
        if self.memory.get_memory_usage() > 10 {
            self.segments.push(SegmentStore::create_from_iterator(
                "temp.seg".to_string(),
                self.memory.iter().map(|(k, v)| (k.to_owned(), v.to_owned()))
            ).unwrap());
            self.memory = MemoryStore::new();
        }

        Ok(())
    }

    fn get(&self, key: &str) -> GetResult {
        match self.memory.get(key)? {
            Some(value) => Ok(Some(value)),
            None => {
                for segment in self.segments.iter().rev() {
                    match segment.get(key) {
                        Ok(Some(value)) => return Ok(Some(value)),
                        _ => {}
                    }
                }
                Ok(None)
            }
        }
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