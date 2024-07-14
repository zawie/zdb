use lib::{database::*, Storage};
use log::{debug, info};
use std::net::TcpStream;
use std::{net::TcpListener, path::PathBuf};
use std::io::{prelude::*, BufReader};

fn main() {

    let mut db = Database::new(PathBuf::from("~/zdb")).expect("Failed to create database");
    let listener = TcpListener::bind("127.0.0.1:7878").unwrap();

    for stream in listener.incoming() {
        let stream = stream.unwrap();

        debug!("Connection established!");
        handle_connection(stream, &mut db);
    }
   
}

fn handle_connection(mut stream: TcpStream, db: &mut Database) {
    let mut buf_reader = BufReader::new(&mut stream);
    let http_request: Vec<_> = buf_reader.by_ref()
        .lines()
        .map(|result| result.unwrap())
        .take_while(|line| !line.is_empty())
        .collect();
    
    debug!("Request: {http_request:#?}");

    let key = http_request[0].split_whitespace().nth(1).unwrap().split("/").nth(1).unwrap();

    let (status, contents) = if http_request[0].starts_with("GET") {
        info!("GET request for key: {key}");
        let contents = db.get(key).unwrap();
        match contents {
            Some(contents) => {
                (200, contents)
            }
            None => {
                (404, String::from("Key not found!"))
            }
        }
    } else if http_request[0].starts_with("POST") {
        info!("POST request for key: {key}");
        let content_length = http_request[http_request.len() - 1].split_whitespace().nth(1).unwrap();
        let content_length = content_length.parse::<usize>().unwrap();
        debug!("Content-Length: {content_length}");

        let mut buf = vec![0u8; content_length];
        if buf_reader.read_exact(&mut buf).is_ok() {
            match String::from_utf8(buf) {
                Ok(value) => {
                    if db.set(key, value.as_str()).is_ok() {
                        (201, value)
                    } else {
                        (500, String::from("Oops! Something went wrong."))
                    }                }
                Err(e) => {
                    (400, format!("Unable to read content: {e}"))
                }
            }
        } else {
            (400, String::from("Unable to read content"))
        }
    } else {
        (405, String::from("Invalid method"))
    };

    let len = contents.len();
    let response = format!("HTTP/1.1 {status}\r\nContent-Length: {len}\r\n\r\n{contents}");
    stream.write_all(response.as_bytes()).unwrap();

    info!("Response: {:#?}", response);

}