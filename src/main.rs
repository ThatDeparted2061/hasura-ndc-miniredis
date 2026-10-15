use std::process::ExitCode;

use ndc_miniredis::MiniRedis;
use ndc_sdk::default_main::default_main;

#[tokio::main]
async fn main() -> ExitCode {
    match default_main::<MiniRedis>().await {
        Ok(()) => ExitCode::SUCCESS,
        Err(err) => {
            eprintln!("{err}");
            ExitCode::FAILURE
        }
    }
}
