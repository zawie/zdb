use std::{error::Error, fs::{self, File, OpenOptions}, io::{self, BufRead, BufReader, Cursor, Read, Seek, Write}, iter::Peekable, path::PathBuf};
use std::str;

use super::GetResult;

use log::{debug, trace};

const BLOCK_SIZE_BYTES: usize = 10_000;
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
    let mut sequence_number_bytes: [u8; 8] = [0; 8];
    reader.read_exact(&mut sequence_number_bytes)?;
    bytes_read += 8;

    // Read each block's first key and store their offset in index
    while !reader.fill_buf()?.is_empty() {
        let (key, block) = read_entry(&mut reader)?;
        index.push((str::from_utf8(key.as_slice())?.to_string(), bytes_read));
        bytes_read += block.len();
    }

    Ok(SegmentStore{
        sequence_number: usize::from_ne_bytes(sequence_number_bytes),
        file_path: file_path,
        index: index,
    })
}

// Merges segment stores into one segment. Duplicate keys are resolved by taking the higehst sequence number key.
pub fn compact(file_path: PathBuf, segments: &mut Vec<SegmentStore>) -> Result<SegmentStore, Box<dyn Error>> {
    segments.sort_by(|a, b| a.get_sequence_number().cmp(&b.get_sequence_number()));

    struct InterIterator {
        iterators: Vec<Peekable<SegmentIterator>>,
    }

    impl Iterator for InterIterator {
        type Item = (String, String);
        
        fn next(&mut self) -> Option<Self::Item> {
            // Pop the current minimum key across all the segments, while resolving duplicates.
            let mut min_element: Option<(String, String)> = None;
            let mut min_iter_index = None;
            // Consider segments in decreasing sequence number order
            for i in (0..=self.iterators.len()-1).rev() {
                let iter = &mut self.iterators[i];
                let curr_element = iter.peek();
                if curr_element.is_none() {
                    continue;
                }
                let curr_element = curr_element.unwrap();
                if min_element.is_none() || min_element.to_owned().unwrap().0 < curr_element.0 {
                    min_element = Some(curr_element.to_owned());
                    min_iter_index = Some(i);
                    continue;
                } else if min_element.to_owned().unwrap().0 == curr_element.0 {
                    // Advanced iterators with lower sequence that match current minimum in order to resolve duplicates.
                    iter.next();
                    continue;
                }
            }
            if !min_iter_index.is_none() {
                // Advanced the minimum iterator to make progress
                self.iterators[min_iter_index?].next();
            }
            
            min_element
        }
    }

   SegmentStore::create_from_iterator(
        file_path,
        segments.iter().map(|s| s.get_sequence_number()).min().unwrap_or(0),
        InterIterator{
            iterators: segments.iter().map(|s| s.iter().peekable()).collect::<Vec<_>>(),
        },
    )

}

impl SegmentStore {

    pub fn get_sequence_number(&self) -> usize {
        self.sequence_number
    }
    
    pub fn iter(&self) -> SegmentIterator {
        if self.index.len() == 0 {
            return SegmentIterator {
                reader: BufReader::new(self.start_from_offset(self.index[0].1).unwrap()),
                block_iterator: BlockIterator::new(&Vec::new()),
            };
        }

        SegmentIterator {
            reader: BufReader::new(self.start_from_offset(self.index[0].1).unwrap()),
            block_iterator: BlockIterator::new(&Vec::new()),

        }
    }

    pub fn create_from_iterator(file_path: PathBuf, sequence_number: usize, sorted_iterator: impl Iterator<Item = (String, String)>) -> Result<SegmentStore, Box<dyn Error>> {
        let mut writer = get_writer(file_path.clone());
        let mut bytes_written = 0usize;

        // Write out the sequence number
        bytes_written += writer.write(&sequence_number.to_ne_bytes())?;

        let mut index = Vec::new();

        let mut buffer = Vec::new();
        let mut first_key: Option<String> = None;

        // Write out key value pairs into blocks which are labled with the first key in the block
        for (k, v) in sorted_iterator {
            if first_key.is_none() {
                index.push((k.to_string(), bytes_written));
                first_key = Some(k.to_string());
            }

            buffer.extend(&encode(k.as_bytes())?);
            buffer.extend(&encode(v.as_bytes())?);

            if buffer.len() > BLOCK_SIZE_BYTES {
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

    pub fn delete(&self) -> io::Result<()> {
        fs::remove_file(self.file_path.to_owned())
    }
}

impl SegmentStore {

    pub fn get(&self, key: &str) -> GetResult {
        // Only scan the block which could contain the desired key value pair
        let key = key.to_string();

        let (block_key, offset) = match closest_element_before(key.clone(), &self.index) {
            Some(k) => k,
            None => return Ok(None),
        };

        debug!("Nearest key for \"{}\" is \"{}\" in segment {}", key, block_key, self.file_path.to_str().unwrap());
        debug!("Reading from offset {} in {}", offset, self.file_path.to_str().unwrap());

        let mut reader = self.start_from_offset(offset)?;
        let (_, block) = read_entry(&mut reader)?;

        trace!("Block size: {}", block.len());

       for (k, v) in BlockIterator::new(&block) {
        if k == key {
            return Ok(Some(v))
        }

       }
        Ok(None)
    }

    fn start_from_offset(&self, offset: usize) -> io::Result<File> {
        let mut file: File = File::open(self.file_path.to_owned())?;
        file.seek(std::io::SeekFrom::Start(offset as u64))?;
        Ok(file)
    }


}

pub struct BlockIterator {
    reader: Cursor<Vec<u8>>
}

impl BlockIterator {
    
    pub fn new(block: &Vec<u8>) -> BlockIterator{
        let data = decompress(&block);
        let reader = Cursor::new(Vec::from(data));

        BlockIterator {
            reader: reader,
        }
    }
}

impl Iterator for BlockIterator {
    type Item = (String, String);

    fn next(&mut self) -> Option<Self::Item> {
        if self.reader.fill_buf().unwrap().is_empty() {
            return None;
        }

        let (k, v) = read_entry(&mut self.reader).unwrap();
        Some((str::from_utf8(k.as_slice()).unwrap().to_string(), str::from_utf8(v.as_slice()).unwrap().to_string()))
    }
}

pub struct SegmentIterator {
    reader: BufReader<File>,
    block_iterator: BlockIterator,
}

impl Iterator for SegmentIterator {
    type Item = (String, String);

    fn next(&mut self) -> Option<Self::Item> {

        match self.block_iterator.next() {
            Some(x) => Some(x),
            None => {
                if self.reader.fill_buf().unwrap().is_empty() {
                    return None;
                }
                let (_, block) = read_entry(&mut self.reader).expect("Failed to reach segment during iteration!");
                self.block_iterator = BlockIterator::new(&block);
                self.block_iterator.next()
            }
        }
    }
}

fn closest_element_before<K:PartialOrd + Clone, V: Clone> (key: K, elements: &Vec<(K,V)>) -> Option<(K,V)> {
    if elements.len() == 0 {
        return None;
    }

    // Binary search
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

    // Ensure we return the closest element before the input
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
    //TODO: Utilize a compression algorithm
    input
}

fn decompress(input: &[u8]) -> &[u8] {
    //TODO: Utilize a decompression algorithm
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
    fn test_compact() {
        let mut state_0 = BTreeMap::new();
        state_0.insert("a", "0");
        state_0.insert("b", "0");

        let mut state_1 = BTreeMap::new();
        state_1.insert("a", "1");
        state_1.insert("c", "1");

        let file_path_0: PathBuf = PathBuf::from("temp_compact_0.seg");
        let segment_0 = SegmentStore::create_from_iterator(
            file_path_0.to_owned(), 
            0,
            state_0.iter().map(|(k, v)| (k.to_string(), v.to_string())))
            .expect("Failed to create first segment!");


        let file_path_1: PathBuf = PathBuf::from("temp_compact_1.seg");
        let segment_1 = SegmentStore::create_from_iterator(
            file_path_1.to_owned(), 
            1,
            state_1.iter().map(|(k, v)| (k.to_string(), v.to_string())))
            .expect("Failed to create second segment!");


        let file_path_compact: PathBuf = PathBuf::from("temp_compact_compacted.seg");
        let compact_segment = compact(
            file_path_compact.to_owned(),
            &mut vec!(segment_0, segment_1)
        ).expect("Failed to compact segments!");
        let _ = fs::remove_file(file_path_0);
        let _ = fs::remove_file(file_path_1);

        for (k,v) in compact_segment.iter() {
            match k.as_str() {
                "a" => assert_eq!(v, "1", "Latest segment should be represented!"),
                "b" => assert_eq!(v, "0", "Keys from segment 0 are not be present!"),
                "c" => assert_eq!(v, "1", "Keys from segment 1 are not be present!"),
                _ => panic!("Unknown key present!"),
            }
        }

        let _ = fs::remove_file(file_path_compact);

    }

    #[test]
    fn test_random_set_gets() {
        let file_path: PathBuf = PathBuf::from("test_temp.seg");
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

    #[test]
    fn test_segment_iterator() {
        let file_path: PathBuf = PathBuf::from("test_temp_iterator.seg");
        let state = random_state(100);
        let segment = SegmentStore::create_from_iterator(
            file_path.to_owned(), 
            0,
            state.iter().map(|(k, v)| (k.to_string(), v.to_string()))
        
        ).unwrap();

        assert_eq!(state.iter().count(), segment.iter().count());

        state.iter().zip(segment.iter()).for_each(|((k1, v1), (k2, v2))| {
            assert_eq!(k1.to_owned(), k2);
            assert_eq!(v1.to_owned(), v2);
        });

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