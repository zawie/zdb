use std::{error::Error, fs::{File, OpenOptions}, io::{self, BufRead, BufReader, Cursor, Read, Seek, Write}};

use std::mem::transmute;

use super::{GetResult};

pub struct SegmentStore {
    file_path: String,  
    index: Vec<(String, usize)>, // (key, offset)
}

impl SegmentStore {

    pub fn create_from_iterator(file_path: String, sorted_iterator: impl Iterator<Item = (String, String)>) -> Result<SegmentStore, Box<dyn Error>> {
        let mut writer = get_writer(&file_path);
        let mut bytes_written = 0usize;

        let mut index = Vec::new();

        let mut buffer = String::new();
        let mut first_key: Option<String> = None;

        for (k, v) in sorted_iterator {
            if first_key.is_none() {
                index.push((k.to_string(), bytes_written));
                first_key = Some(k.to_string());
            }

            buffer.push_str(&encode_string(k.as_str())?);
            buffer.push_str(&encode_string(v.as_str())?);

            if buffer.len() > 1000 {
                bytes_written += writer.write(encode_string(first_key.unwrap().as_str())?.as_bytes())?;
                bytes_written += writer.write(encode_string(&compress(&buffer))?.as_bytes())?;
                
                buffer.clear();
                first_key = None;
            }
        }

        if buffer.len() > 0 {
            writer.write(encode_string(first_key.unwrap().as_str())?.as_bytes())?;
            writer.write(encode_string(&compress(&buffer))?.as_bytes())?;
        }

        Ok(SegmentStore{
            file_path: file_path,
            index: index,
        })
    }
}

impl SegmentStore {

    pub fn get(&self, key: &str) -> GetResult {
        let key = key.to_string();

        let mut low = 0;
        let mut high = self.index.len() - 1;

        while low <= high {
            let mid = (low + high) / 2;
            let (mid_key, offset) = &self.index[mid];

            if *mid_key == key {
                // Key found, read the corresponding value from the file
                let mut reader = self.start_from_offset(*offset)?;
                let (_, block) = read_entry(&mut reader)?;

                let data = decompress(&block);
                let mut reader: BufReader<&[u8]> = BufReader::new(data.as_bytes());
                while !reader.fill_buf()?.is_empty() {
                    let (k, v) = read_entry(&mut reader)?;
                    if k == key {
                        return Ok(Some(v));
                    }
                }

                return Ok(None);
            } else if *mid_key < key {
                low = mid + 1;
            } else {
                high = mid - 1;
            }
        }

        // Key not found
        Ok(None)
    }

    fn start_from_offset(&self, offset: usize) -> io::Result<File> {
        let mut file: File = File::open(self.file_path.to_owned())?;
        file.seek(std::io::SeekFrom::Start(offset as u64))?;
        Ok(file)
    }


}

fn read_entry(reader: &mut impl Read) -> Result<(String, String), Box<dyn Error>> {
    let key = decode_string(reader)?;
    let value = decode_string(reader)?;

    Ok((key, value))
}

fn decode_string(reader: &mut impl Read) -> Result<String, Box<dyn Error>> {
    let mut len_bytes = [0u8; 8];
    reader.read_exact(&mut len_bytes)?;

    let len = usize::from_ne_bytes(len_bytes);

    let mut buffer = vec![0u8; len];
    reader.read_exact(&mut buffer)?;

    Ok(String::from_utf8_lossy(&buffer).to_string())
}

fn encode_string(input_string: &str) -> Result<String, Box<dyn Error>> {
    let mut entry = String::new();

    let len_bytes: [u8; 8] = input_string.len().to_ne_bytes();
    let len_string = String::from_utf8_lossy(&len_bytes).to_string();

    entry.push_str(&len_string);
    entry.push_str(input_string);

    Ok(entry)
}

fn get_writer(file_path: &str) -> File {
    OpenOptions::new()
        .write(true)
        .append(true)
        .create(true)
        .open(file_path)
        .unwrap()
}

fn compress(input: &String) -> String {
    input.to_owned()
}

fn decompress(input: &String) -> String {
    input.to_owned()
}