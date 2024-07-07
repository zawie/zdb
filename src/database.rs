use std::{fs, path::PathBuf};
use log::warn;
use uuid::Uuid;
use crate::{log_store::LogStore, memory_store::MemoryStore, segment_store::{SegmentStore, load_from_file}};
use super::{Storage, SetResult, GetResult};

const MAX_MEMORY_USAGE: usize = 100_000;
pub struct Database {
    directory: PathBuf,
    memory: MemoryStore,
    log: LogStore,
    segments: Vec<SegmentStore>,
}

impl Database {
    pub fn new(directory: PathBuf) -> Result<Database, Box<dyn std::error::Error>> {
        fs::create_dir_all(directory.to_owned())?;

        let paths = fs::read_dir(directory.to_owned()).unwrap();
        let mut segments = Vec::new();

        for path in paths {
            let path = path.unwrap();
            if path.file_name().as_os_str().to_str().unwrap().ends_with(".seg") {
                segments.push(load_from_file(path.path())?);
            }
        }

        segments.sort_by(|a, b| a.get_sequence_number().cmp(&b.get_sequence_number()));

        let mut db = Database {
            memory: MemoryStore::new(),
            log: LogStore::init(directory.join("write.log")),
            segments: segments,
            directory: directory,
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
        if self.memory.get_memory_usage() > MAX_MEMORY_USAGE {
            self.segments.push(SegmentStore::create_from_iterator(
                self.directory.join(self.directory.join(format!("{}.seg", Uuid::new_v4().to_string()))),
                self.segments.iter().map(|s| s.get_sequence_number()).max().unwrap_or(0) + 1,
                self.memory.iter().map(|(k, v)| (k.to_owned(), v.to_owned()))
            ).unwrap());
            self.memory = MemoryStore::new();
            self.log.flush()?;
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
        let mut db = Database::new(PathBuf::from("/tmp/zdb_test_database")).expect("Failed to create database");

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