//! Generate the v3 engine's OpenDD metadata from the connector configuration:
//! `metadata <config-dir> <connector-url> > engine/metadata.json`
//!
//! The connector's schema and capabilities are embedded in the
//! DataConnectorLink, so the metadata can't drift from what the connector serves.
//! Every collection becomes a model (`users`, `usersById`) with filters and
//! ordering on every field; every procedure becomes a mutation command.

use std::path::Path;

use ndc_miniredis::{config, schema};
use serde_json::{json, Value};

fn pascal(s: &str) -> String {
    s.split('_').map(|p| p[..1].to_uppercase() + &p[1..]).collect()
}

fn main() {
    let args: Vec<String> = std::env::args().collect();
    let dir = args.get(1).map(String::as_str).unwrap_or("config");
    let url = args.get(2).map(String::as_str).unwrap_or("http://connector:8100");
    let c = config::load(Path::new(dir)).unwrap_or_else(|e| panic!("{e}"));
    let dc = "miniredis";
    let mut objects: Vec<Value> = vec![json!({
        "kind": "DataConnectorLink", "version": "v1",
        "definition": {
            "name": dc,
            "url": {"singleUrl": {"value": url}},
            "schema": {
                "version": "v0.2",
                "schema": schema::schema(&c),
                "capabilities": {"version": "0.2.12", "capabilities": schema::capabilities()}
            }
        }
    })];
    for (ndc, opendd) in [("String", "String"), ("Int", "Int"), ("Float", "Float"), ("Boolean", "Boolean")] { // BigInt only types sum aggregates, which this metadata does not expose
        objects.push(json!({"kind": "DataConnectorScalarRepresentation", "version": "v1", "definition": {
            "dataConnectorName": dc, "dataConnectorScalarType": ndc, "representation": opendd,
            "graphql": {"comparisonExpressionTypeName": format!("{ndc}ComparisonExp")}
        }}));
    }
    for (name, col) in &c.collections {
        let ty = pascal(name);
        let mut fields = vec![("id".to_string(), "String!".to_string())];
        fields.extend(col.columns.iter().map(|(f, t)| (f.clone(), t.name().to_string())));
        let names: Vec<&String> = fields.iter().map(|(f, _)| f).collect();
        objects.push(json!({"kind": "ObjectType", "version": "v1", "definition": {
            "name": name,
            "fields": fields.iter().map(|(f, t)| json!({"name": f, "type": t})).collect::<Vec<_>>(),
            "graphql": {"typeName": ty, "inputTypeName": format!("{ty}Input")},
            "dataConnectorTypeMapping": [{"dataConnectorName": dc, "dataConnectorObjectType": name,
                "fieldMapping": names.iter().map(|f| (f.to_string(), json!({"column": {"name": f}}))).collect::<serde_json::Map<_, _>>()}]
        }}));
        objects.push(json!({"kind": "ObjectBooleanExpressionType", "version": "v1", "definition": {
            "name": format!("{name}_bool_exp"), "objectType": name, "dataConnectorName": dc, "dataConnectorObjectType": name,
            "comparableFields": names.iter().map(|f| json!({"fieldName": f, "operators": {"enableAll": true}})).collect::<Vec<_>>(),
            "graphql": {"typeName": format!("{ty}BoolExp")}
        }}));
        objects.push(json!({"kind": "Model", "version": "v1", "definition": {
            "name": name, "objectType": name,
            "source": {"dataConnectorName": dc, "collection": name},
            "filterExpressionType": format!("{name}_bool_exp"),
            "orderableFields": names.iter().map(|f| json!({"fieldName": f, "orderByDirections": {"enableAll": true}})).collect::<Vec<_>>(),
            "graphql": {
                "selectMany": {"queryRootField": name},
                "selectUniques": [{"queryRootField": format!("{name}ById"), "uniqueIdentifier": ["id"]}],
                "orderByExpressionType": format!("{ty}OrderBy")
            }
        }}));
        objects.push(json!({"kind": "TypePermissions", "version": "v1", "definition": {
            "typeName": name, "permissions": [{"role": "admin", "output": {"allowedFields": names}}]
        }}));
        objects.push(json!({"kind": "ModelPermissions", "version": "v1", "definition": {
            "modelName": name, "permissions": [{"role": "admin", "select": {"filter": null}}]
        }}));
        for (verb, args, out) in [
            ("insert", fields.clone(), format!("{name}!")),
            ("delete", vec![("id".to_string(), "String!".to_string())], name.clone()),
        ] {
            let cmd = format!("{verb}_{name}");
            objects.push(json!({"kind": "Command", "version": "v1", "definition": {
                "name": cmd,
                "arguments": args.iter().map(|(a, t)| json!({"name": a, "type": t})).collect::<Vec<_>>(),
                "outputType": out,
                "source": {"dataConnectorName": dc, "dataConnectorCommand": {"procedure": cmd},
                    "argumentMapping": args.iter().map(|(a, _)| (a.clone(), json!(a))).collect::<serde_json::Map<_, _>>()},
                "graphql": {"rootFieldName": format!("{verb}{ty}"), "rootFieldKind": "Mutation"}
            }}));
            objects.push(json!({"kind": "CommandPermissions", "version": "v1", "definition": {
                "commandName": cmd, "permissions": [{"role": "admin", "allowExecution": true}]
            }}));
        }
    }
    println!("{}", serde_json::to_string_pretty(&json!({"version": "v2", "subgraphs": [{"name": "default", "objects": objects}]})).unwrap());
}
