use lib::database;

fn main() {
    println!("Hello, world!");

    let db = database::Database::new();

    let key = vec![1, 2, 3];
    let value = vec![4, 5, 6];

    db.set(&key, &value)
        .expect("Failed to set!");    
}
