use std::{error::Error, fs::{File, OpenOptions}, io::{self, BufRead, BufReader, Read, Seek, Write}, path::PathBuf};
use std::str;

use super::{GetResult};

use log::{debug};

const BLOCK_SIZE: usize = 10000;
pub struct SegmentStore {
    sequence_number: usize,
    file_path: PathBuf,  
    index: Vec<(String, usize)>, // (key, offset)
}

pub fn load_from_file(file_path: PathBuf) -> Result<SegmentStore, Box<dyn Error>> {
    let mut index = Vec::new();
    let mut reader: BufReader<File> = BufReader::new(File::open(file_path.to_owned())?);
    let mut bytes_read = 0;

    // Read in the sequence number
    let mut sequene_number_bytes: [u8; 8] = [0; 8];
    reader.read_exact(&mut sequene_number_bytes)?;
    bytes_read += 8;

    while !reader.fill_buf()?.is_empty() {
        let (key, block) = read_entry(&mut reader)?;
        index.push((str::from_utf8(key.as_slice())?.to_string(), bytes_read));
        bytes_read += block.len();
    }


    Ok(SegmentStore{
        sequence_number: usize::from_ne_bytes(sequene_number_bytes),
        file_path: file_path,
        index: index,
    })
}

impl SegmentStore {

    pub fn create_from_iterator(file_path: PathBuf, sequence_number: usize, sorted_iterator: impl Iterator<Item = (String, String)>) -> Result<SegmentStore, Box<dyn Error>> {
        let mut writer = get_writer(file_path.clone());
        let mut bytes_written = 0usize;

        bytes_written += writer.write(&sequence_number.to_ne_bytes())?;

        let mut index = Vec::new();

        let mut buffer = Vec::new();
        let mut first_key: Option<String> = None;

        for (k, v) in sorted_iterator {
            if first_key.is_none() {
                index.push((k.to_string(), bytes_written));
                first_key = Some(k.to_string());
            }

            buffer.extend(&encode(k.as_bytes())?);
            buffer.extend(&encode(v.as_bytes())?);

            if buffer.len() > BLOCK_SIZE {
                debug!("Writing block of size {} with first key \"{}\"", buffer.len(), first_key.clone().unwrap());
                bytes_written += writer.write(encode(first_key.unwrap().as_bytes())?.as_slice())?;
                bytes_written += writer.write(encode(compress(&buffer))?.as_slice())?;
                
                buffer.clear();
                first_key = None;
            }
        }

        if buffer.len() > 0 {
            debug!("Writing final block of size {} with first key \"{}\"", buffer.len(), first_key.clone().unwrap());
            bytes_written += writer.write(encode(first_key.unwrap().as_bytes())?.as_slice())?;
            bytes_written += writer.write(encode(compress(&buffer))?.as_slice())?;
        }

        debug!("Finished writing segment to {}. Wrote {} bytes in {} blocks", file_path.to_str().unwrap(), bytes_written, index.len());

        Ok(SegmentStore{
            sequence_number: sequence_number,
            file_path: file_path,
            index: index,
        })
    }
}

impl SegmentStore {

    pub fn get(&self, key: &str) -> GetResult {
        debug!("Getting key from segment");

        let key = key.to_string();

        let (block_key, offset) = match closest_element_before(key.clone(), &self.index) {
            Some(k) => k,
            None => return Ok(None),
        };

        debug!("Nearest key for \"{}\" is \"{}\" in segment {}", key, block_key, self.file_path.to_str().unwrap());
        debug!("Reading from offset {} in {}", offset, self.file_path.to_str().unwrap());

        let mut reader = self.start_from_offset(offset)?;
        let (_, block) = read_entry(&mut reader)?;

        debug!("Block size: {}", block.len());


        let data = decompress(&block);
        let mut reader: BufReader<&[u8]> = BufReader::new(data);
        while !reader.fill_buf()?.is_empty() {
            let (k, v) = read_entry(&mut reader)?;
            if k == key.as_bytes() {
                return Ok(Some(
                    match str::from_utf8(v.as_slice()) {
                        Ok(v) => String::from(v),
                        Err(e) => panic!("Invalid UTF-8 sequence: {}", e),
                    }
                ));
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

fn closest_element_before<K:PartialOrd + Clone, V: Clone> (key: K, elements: &Vec<(K,V)>) -> Option<(K,V)> {
    if elements.len() == 0 {
        return None;
    }

    let mut low = 0;
    let mut high = elements.len() - 1;

    let mut mid: usize = 0;
    while low <= high {
        mid = (low + high) / 2;
        let mid_key = &elements[mid].0;

        if *mid_key == key {
            break;
        } else if *mid_key < key {
            low = mid + 1;
        } else {
            if mid == 0 {
                break;
            }
            high = mid - 1;
        }
    }

    if elements[mid].0 > key {
        if mid == 0 {
            return None;
        }            
        mid -= 1;
    }
    return Some(elements[mid].clone());
}

fn read_entry(reader: &mut impl Read) -> Result<(Vec<u8>, Vec<u8>,), Box<dyn Error>> {
    let key = decode(reader)?;
    let value = decode(reader)?;

    Ok((key, value))
}

fn decode(reader: &mut impl Read) -> Result<Vec<u8>, Box<dyn Error>> {
    let mut buffer = [0u8; 8];
    reader.read_exact(&mut buffer)?;

    let len = usize::from_ne_bytes(buffer);
    debug!("Reading string of length {}", len);
    let mut buffer = vec![0u8; len];
    reader.read_exact(&mut buffer)?;

    Ok(buffer)
}

fn encode(input_string: &[u8]) -> Result<Vec<u8>, Box<dyn Error>> {
    let mut entry = Vec::new();

    let len_bytes: [u8; 8] = input_string.len().to_ne_bytes();

    entry.extend_from_slice(&len_bytes);
    entry.extend_from_slice(input_string);

    Ok(entry)
}

fn get_writer(file_path: PathBuf) -> File {
    OpenOptions::new()
        .write(true)
        .append(true)
        .create(true)
        .open(file_path)
        .unwrap()
}

fn compress(input: &[u8]) -> &[u8] {
    input
}

fn decompress(input: &[u8]) -> &[u8] {
    input
}

#[cfg(test)]
mod tests {
    use std::{collections::BTreeMap, fs};

    use super::*;
    use rand::{distributions::Alphanumeric, Rng}; // 0.8

    #[test]
    fn test_closest_element_before() {
        let elements: Vec<(&str,())> = [].to_vec();
        assert_eq!(closest_element_before("Oh no!", &elements), None);

        let elements = [("b", ()), ("c", ()), ("d",()), ("e",()), ("f",()), ("g",()), ("h",()), ("i",())].to_vec();
         assert_eq!(closest_element_before("a", &elements), None);
         assert_eq!(closest_element_before("b", &elements).unwrap(), ("b",()));
         assert_eq!(closest_element_before("bb", &elements).unwrap(), ("b",()));
         assert_eq!(closest_element_before("e", &elements).unwrap(), ("e",()));
         assert_eq!(closest_element_before("ee",&elements).unwrap(), ("e",()));
         assert_eq!(closest_element_before("i", &elements).unwrap(), ("i",()));
         assert_eq!(closest_element_before("ii",&elements).unwrap(), ("i",()));
    }

    #[test]
    fn test_encode_decode() {
        let input = sample_string(1,1000);
        let encoded = encode(input.as_bytes()).unwrap();
        let mut reader: BufReader<&[u8]> = BufReader::new(encoded.as_slice());
        let decoded = decode(&mut reader).unwrap();

        assert_eq!(input, str::from_utf8(decoded.as_slice()).unwrap());
    }

    #[test]
    fn test_random() {
        let file_path = PathBuf::from("test_temp.seg");
        let state = random_state(100);

        let segment = SegmentStore::create_from_iterator(
            file_path.to_owned(), 
            0,
            state.iter().map(|(k, v)| (k.to_string(), v.to_string()))
        
        ).unwrap();

        for (k, v) in state.clone() {
            let result = segment.get(&k).unwrap();
            assert_eq!(result.unwrap(), v.to_string());
        }
    
        let segment = load_from_file(file_path.to_owned()).unwrap();

        for (k, v) in state {
            let result = segment.get(&k).unwrap();
            assert_eq!(result.unwrap(), v.to_string());
        }

        let _ = fs::remove_file(file_path);
    }

    fn random_state(count: u32) -> BTreeMap<String, String> {
        let mut entries = BTreeMap::new();
        for _ in 0..count {
            entries.insert(sample_string(1, 10), sample_string(1, 20));
        }
        
        entries
    }

    fn sample_string(min: usize, max: usize) -> String {        
        let mut entropy = rand::thread_rng();
        let length: usize = entropy.gen_range(min..max);

        entropy.sample_iter(&Alphanumeric)
                .take(length)
                .map(char::from)
                .collect()
    }
}