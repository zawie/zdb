use lib::{database, Storage};
use std::{io, path::PathBuf};

fn main() {

    let mut db = database::Database::new(PathBuf::from("/tmp/zdb")).expect("Failed to create database");

    loop {
        let mut input = String::new();

        io::stdin()
            .read_line(&mut input)
            .expect("Failed to read line");

        let chunks: Vec<&str> = input
            .trim()
            .split(" ")
            .collect();

        match chunks[0] {
            "get" => {
                if chunks.len() != 2 {
                    println!("Usage: get <key>");
                    continue;
                }
                let r = db.get(&chunks[1]);
                match r {
                    Ok(_) => {
                        match r.unwrap() {
                            Some(value) => println!("{}", value),
                            None => println!(">> Key not found!")
                        }
                    }
                    Err(e) => println!(">> Error getting value: {}", e)
                }
            }
            "set" => {
                if chunks.len() != 3 {
                    println!("Usage: set <key> <value>");
                    continue;
                }
                let r = db.set(chunks[1], chunks[2]);
                match r {
                    Ok(_) => println!(">> Value set!"),
                    Err(e) => println!(">> Error setting value: {}", e)
                }
            }
            _ => {
                println!("Unknown command! Known commands: get <key>, set <key> <");
            }
        }
    }
}
