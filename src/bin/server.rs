use lib::{database::*, Storage};
use log::{debug, info, trace};
use std::fs;
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
    
    println!("Request: {http_request:#?}");

    let key = http_request[0].split_whitespace().nth(1).unwrap().split("/").nth(1).unwrap();

    let response = if http_request[0].starts_with("GET") {
        print!("GET request for key: {key}");
        let contents = db.get(key).unwrap();
        match contents {
            Some(contents) => {
                let status_line = "HTTP/1.1 200 OK";
                let length = contents.len();
                format!("{status_line}\r\nContent-Length: {length}\r\n\r\n{contents}")
            }
            None => {
                String::from("HTTP/1.1 404\r\nContent-Length: 0\r\n\r\n")
            }
        }
    } else if http_request[0].starts_with("POST") {
        println!("POST request for key: {key}");
        let content_length = http_request[http_request.len() - 1].split_whitespace().nth(1).unwrap();
        let content_length = content_length.parse::<usize>().unwrap();
        println!("Content-Length: {content_length}");

        let mut buf = vec![0u8; content_length];
        buf_reader.read_exact(&mut buf).unwrap();
        db.set(key, &String::from_utf8(buf).unwrap()).unwrap();

        String::from("HTTP/1.1 200\r\nContent-Length: 0\r\n\r\n")
    } else {
        String::from("HTTP/1.1 405\r\nContent-Length: 0\r\n\r\n")
    };

    stream.write_all(response.as_bytes()).unwrap();

    println!("Response: {:#?}", response);

}