use std::{fs::{File, OpenOptions}, io::{self, BufRead, BufReader, Write}, path::PathBuf};

use crate::{GetResult, SetResult, Storage};

 pub struct LogStore {
    file_path: PathBuf,
    writer: File,
}

impl LogStore {

    pub fn init(file_path: PathBuf) -> LogStore {
        LogStore {
            file_path: file_path.to_owned(),
            writer: OpenOptions::new()
                .write(true)
                .append(true)
                .create(true)
                .open(file_path)
                .unwrap(),
        }
    }

    pub fn iter(&self) -> io::Result<LogStoreIterator> {
        let file = File::open(&self.file_path)?;
        Ok(LogStoreIterator {
            lines:  io::BufReader::new(file).lines()
        })
    }

    pub fn flush(&mut self) -> io::Result<()> {
        self.writer.sync_all()?;
        self.writer = std::fs::OpenOptions::new().truncate(true).write(true).open(self.file_path.to_owned())?;

        Ok(())
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
                chars.next();
                match chars.peek() {
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

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn test_serialize() {
        let x = "Hello\\\nWorld\t!\\n\n";
        let y = deserialize(&serialize(x));
        assert!(x == y, "Expected {} but got {}", x, y);
    }
}