//! Load deterministic sample data: `seed [redis-url] [users] [orders]`.
//! Writes hashes and index sets exactly as the connector's insert procedures do.

use std::env;

const CITIES: [&str; 6] = ["Bengaluru", "Mumbai", "Delhi", "Chennai", "Pune", "Kolkata"];
const NAMES: [&str; 8] = ["Asha", "Ravi", "Meera", "Kabir", "Isha", "Arjun", "Zara", "Dev"];
const STATUS: [&str; 4] = ["placed", "shipped", "delivered", "cancelled"];

#[tokio::main]
async fn main() -> redis::RedisResult<()> {
    let args: Vec<String> = env::args().collect();
    let url = args.get(1).cloned().unwrap_or_else(|| "redis://127.0.0.1:6380".into());
    let users: usize = args.get(2).and_then(|s| s.parse().ok()).unwrap_or(1000);
    let orders: usize = args.get(3).and_then(|s| s.parse().ok()).unwrap_or(5000);
    let mut conn = redis::Client::open(url)?.get_multiplexed_async_connection().await?;
    let mut x: u64 = 42;
    let mut next = move || {
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        x
    };
    for chunk in (0..users).collect::<Vec<_>>().chunks(500) {
        let mut pipe = redis::pipe();
        for &i in chunk {
            let id = format!("{}", i + 1);
            let name = format!("{} {}", NAMES[i % NAMES.len()], i + 1);
            let mut hset = redis::cmd("HSET");
            hset.arg(format!("user:{id}"))
                .arg("name").arg(&name)
                .arg("email").arg(format!("user{}@example.com", i + 1))
                .arg("city").arg(CITIES[(next() % 6) as usize])
                .arg("score").arg(format!("{:.2}", (next() % 10000) as f64 / 100.0))
                .arg("active").arg(if next() % 4 == 0 { "false" } else { "true" });
            if i % 10 != 0 {
                hset.arg("age").arg(18 + next() % 60); // every 10th user has no age: nullable columns stay honest
            }
            pipe.add_command(hset).ignore();
            pipe.cmd("SADD").arg("user:__index").arg(&id).ignore();
        }
        pipe.query_async::<()>(&mut conn).await?;
    }
    for chunk in (0..orders).collect::<Vec<_>>().chunks(500) {
        let mut pipe = redis::pipe();
        for &i in chunk {
            let id = format!("{}", 100000 + i);
            pipe.cmd("HSET").arg(format!("order:{id}"))
                .arg("user_id").arg(format!("{}", next() % users as u64 + 1))
                .arg("status").arg(STATUS[(next() % 4) as usize])
                .arg("total").arg(format!("{:.2}", (next() % 500000) as f64 / 100.0))
                .arg("items").arg(1 + next() % 9)
                .ignore();
            pipe.cmd("SADD").arg("order:__index").arg(&id).ignore();
        }
        pipe.query_async::<()>(&mut conn).await?;
    }
    println!("seeded {users} users and {orders} orders");
    Ok(())
}
