//! The connector's configuration: where Redis is, and which hashes form
//! which collection. `configuration.json` lives in the directory passed with
//! `--configuration` (HASURA_CONFIGURATION_DIRECTORY).

use std::collections::BTreeMap;
use std::path::Path;

use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Configuration {
    /// e.g. redis://127.0.0.1:6380. REDIS_URL overrides it.
    pub redis_url: String,
    pub collections: BTreeMap<String, Collection>,
}

/// A collection is every hash whose key is `key_prefix + id`. The ids live in
/// a set (`index_key`), because Redis-compatible stores without KEYS or SCAN
/// (mini-redis-go among them) can't enumerate keys any other way.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Collection {
    #[serde(default)]
    pub description: Option<String>,
    pub key_prefix: String,
    #[serde(default)]
    pub index_key: Option<String>,
    /// Hash field name -> scalar type. Every row also has a String `id`.
    pub columns: BTreeMap<String, ScalarType>,
}

impl Collection {
    pub fn index_key(&self) -> String {
        self.index_key.clone().unwrap_or_else(|| format!("{}__index", self.key_prefix))
    }
    pub fn key(&self, id: &str) -> String {
        format!("{}{}", self.key_prefix, id)
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum ScalarType {
    String,
    Int,
    Float,
    Boolean,
}

impl ScalarType {
    pub fn name(self) -> &'static str {
        match self {
            ScalarType::String => "String",
            ScalarType::Int => "Int",
            ScalarType::Float => "Float",
            ScalarType::Boolean => "Boolean",
        }
    }

    /// Parse a hash field (Redis stores bytes) into JSON; unparseable is null.
    pub fn parse(self, raw: &str) -> serde_json::Value {
        use serde_json::Value;
        match self {
            ScalarType::String => Value::String(raw.to_string()),
            ScalarType::Int => raw.parse::<i32>().map(Value::from).unwrap_or(Value::Null), // Int32: NDC serialises Int64 as strings
            ScalarType::Float => raw.parse::<f64>().ok().and_then(serde_json::Number::from_f64).map(Value::Number).unwrap_or(Value::Null),
            ScalarType::Boolean => match raw {
                "true" | "1" => Value::Bool(true),
                "false" | "0" => Value::Bool(false),
                _ => Value::Null,
            },
        }
    }

    /// Render a JSON argument back into the string stored in the hash.
    pub fn render(self, v: &serde_json::Value) -> Option<String> {
        match (self, v) {
            (_, serde_json::Value::Null) => None,
            (ScalarType::String, serde_json::Value::String(s)) => Some(s.clone()),
            (ScalarType::Int | ScalarType::Float, serde_json::Value::Number(n)) => Some(n.to_string()),
            (ScalarType::Boolean, serde_json::Value::Bool(b)) => Some(b.to_string()),
            _ => None,
        }
    }
}

pub fn load(dir: &Path) -> Result<Configuration, String> {
    let path = dir.join("configuration.json");
    let text = std::fs::read_to_string(&path).map_err(|e| format!("{}: {e}", path.display()))?;
    let mut c: Configuration = serde_json::from_str(&text).map_err(|e| format!("{}: {e}", path.display()))?;
    if let Ok(url) = std::env::var("REDIS_URL") {
        c.redis_url = url;
    }
    for (name, col) in &c.collections {
        if col.columns.contains_key("id") {
            return Err(format!("collection {name}: `id` is the key suffix and can't also be a column"));
        }
    }
    Ok(c)
}
