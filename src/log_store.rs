use std::{fs::{File, OpenOptions}, io::{self, BufRead, BufReader, Write}};

use crate::{GetResult, SetResult, Storage};

 pub struct LogStore {
    file_path: String,
    writer: File,
}

impl LogStore {

    pub fn init() -> LogStore {
        let f = "temp.log".to_string();

        LogStore {
            file_path: f.to_owned(),
            writer: OpenOptions::new()
                .write(true)
                .append(true)
                .create(true)
                .open(f)
                .unwrap(),
        }
    }

    pub fn iter(&self) -> io::Result<LogStoreIterator> {
        let file = File::open(&self.file_path)?;
        Ok(LogStoreIterator {
            lines:  io::BufReader::new(file).lines()
        })
    }

}

impl Storage for LogStore {

    fn set(&mut self, key: &str, value: &str) -> SetResult {

        
        let mut entry = String::new();
        entry.push_str(&serialize(key));
        entry.push_str("\t");
        entry.push_str(&serialize(value));
        entry.push_str("\n");

        match self.writer.write(entry.as_bytes()) {
            Ok(size) => {
                if size == entry.len() {
                    Ok(())
                } else {
                    Err("Failed to write all bytes into log".into())
                }
            }
            Err(e) => Err(e.into())
        }
    }

    fn get(&self, key: &str) -> GetResult {
        let entries = match self.iter() {
            Ok(entries) => entries,
            Err(e) => return Err(e.into())
        };

        let mut latest: Option<String> = None;
        for (k, v) in entries {
            if k == key {
                latest = Some(v);
            }
        }

        Ok(latest)
    }
}

fn serialize(input: &str) -> String {
    input.replace("\\","\\\\")
         .replace("\n", "\\n")
         .replace("\t", "\\t")
}


fn deserialize(input: &str) -> String {
    let mut result = String::new();

    let mut chars = input.chars().peekable();

    while let Some(&ch) = chars.peek() {        
        match ch {
            '\\' => {
                input.chars().next();
                match input.chars().peekable().peek() {
                    Some('n') => {
                        result.push('\n');
                    }
                    Some('t') => {
                        result.push('\t');
                    }
                    Some('\\') => {
                        result.push('\\');
                    }
                    _ => {
                        result.push('\\');
                        result.push(ch);
                    }
                }
            }
            _ => {
                result.push(ch);
            }
        }

        chars.next();
    }

    result
}

pub struct LogStoreIterator {
    lines: std::io::Lines<BufReader<File>>
}

impl Iterator for LogStoreIterator {
    type Item = (String, String);
    
    fn next(&mut self) -> Option<Self::Item> {
        let line = self.lines.next();

        match line {
            Some(Ok(line)) => {
                let mut parts = line.split("\t");
                let key = parts.next().unwrap();
                let value = parts.next().unwrap();
                Some((deserialize(key), deserialize(value)))
            }
            _ => None
        }
    }
}