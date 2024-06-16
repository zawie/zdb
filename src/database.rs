use std::error::Error;

type SetResult = std::result::Result<(), Box<dyn Error>>;
type GetResult<T> = std::result::Result<T, Box<dyn Error>>;

pub struct Database {
}

impl Database {
    pub fn new() -> Database {
        Database {}
    }

    pub fn set(&self, key: &Vec<u8>, value: &Vec<u8>) -> SetResult {

        return Err("Not implemented yet!".into())
    }

    pub fn get(&self, key: &Vec<u8>) -> GetResult<Vec<u8>> {
    
        return Err("Not implemented yet!".into())
    }
}