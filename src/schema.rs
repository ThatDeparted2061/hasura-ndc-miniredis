//! The NDC schema: four scalar types, one object type and collection per
//! configured collection, and insert/delete procedures for each.

use std::collections::BTreeMap;

use ndc_sdk::models::{
    self, AggregateFunctionDefinition as Agg, ComparisonOperatorDefinition as Op, TypeRepresentation,
};

use crate::config::Configuration;

fn named(t: &str) -> models::Type {
    models::Type::Named { name: t.into() }
}

fn nullable(t: models::Type) -> models::Type {
    models::Type::Nullable { underlying_type: Box::new(t) }
}

fn scalar(rep: TypeRepresentation, ops: &[(&str, Op)], aggs: &[(&str, Agg)]) -> models::ScalarType {
    models::ScalarType {
        representation: rep,
        aggregate_functions: aggs.iter().map(|(n, a)| ((*n).into(), a.clone())).collect(),
        comparison_operators: ops.iter().map(|(n, o)| ((*n).into(), o.clone())).collect(),
        extraction_functions: BTreeMap::new(),
    }
}

fn scalar_types() -> BTreeMap<models::ScalarTypeName, models::ScalarType> {
    let ordered = [
        ("_eq", Op::Equal),
        ("_in", Op::In),
        ("_lt", Op::LessThan),
        ("_lte", Op::LessThanOrEqual),
        ("_gt", Op::GreaterThan),
        ("_gte", Op::GreaterThanOrEqual),
    ];
    let minmax = [("min", Agg::Min), ("max", Agg::Max)];
    let mut string_ops = ordered.to_vec();
    string_ops.extend([
        ("_contains", Op::Contains),
        ("_icontains", Op::ContainsInsensitive),
        ("_starts_with", Op::StartsWith),
    ]);
    let numeric = |sum: &str| {
        vec![
            ("min", Agg::Min),
            ("max", Agg::Max),
            ("sum", Agg::Sum { result_type: sum.into() }),
            ("avg", Agg::Average { result_type: "Float".into() }),
        ]
    };
    BTreeMap::from([
        ("String".into(), scalar(TypeRepresentation::String, &string_ops, &minmax)),
        ("Int".into(), scalar(TypeRepresentation::Int32, &ordered, &numeric("BigInt"))),
        // the result of summing Int columns; Int64 values travel as JSON strings
        ("BigInt".into(), scalar(TypeRepresentation::Int64, &[("_eq", Op::Equal)], &[])),
        ("Float".into(), scalar(TypeRepresentation::Float64, &ordered, &numeric("Float"))),
        ("Boolean".into(), scalar(TypeRepresentation::Boolean, &[("_eq", Op::Equal), ("_in", Op::In)], &[])),
    ])
}

pub fn schema(c: &Configuration) -> models::SchemaResponse {
    let mut object_types = BTreeMap::new();
    let mut collections = vec![];
    let mut procedures = vec![];
    for (name, col) in &c.collections {
        let mut fields = BTreeMap::from([(
            "id".into(),
            models::ObjectField { description: Some(format!("Key suffix: the row is the hash `{}<id>`", col.key_prefix)), r#type: named("String"), arguments: BTreeMap::new() },
        )]);
        for (field, ty) in &col.columns {
            fields.insert(field.as_str().into(), models::ObjectField { description: None, r#type: nullable(named(ty.name())), arguments: BTreeMap::new() });
        }
        object_types.insert(
            name.as_str().into(),
            models::ObjectType { description: col.description.clone(), fields, foreign_keys: BTreeMap::new() },
        );
        collections.push(models::CollectionInfo {
            name: name.as_str().into(),
            description: col.description.clone(),
            arguments: BTreeMap::new(),
            collection_type: name.as_str().into(),
            uniqueness_constraints: BTreeMap::from([(format!("{name}_by_id"), models::UniquenessConstraint { unique_columns: vec!["id".into()] })]),
            relational_mutations: None,
        });

        let mut args = BTreeMap::from([("id".into(), models::ArgumentInfo { description: None, argument_type: named("String") })]);
        for (field, ty) in &col.columns {
            args.insert(field.as_str().into(), models::ArgumentInfo { description: None, argument_type: nullable(named(ty.name())) });
        }
        procedures.push(models::ProcedureInfo {
            name: format!("insert_{name}").into(),
            description: Some(format!("Insert or replace a {name} row; null fields are removed")),
            arguments: args,
            result_type: named(name),
        });
        procedures.push(models::ProcedureInfo {
            name: format!("delete_{name}").into(),
            description: Some(format!("Delete a {name} row by id; returns it, or null if it didn't exist")),
            arguments: BTreeMap::from([("id".into(), models::ArgumentInfo { description: None, argument_type: named("String") })]),
            result_type: nullable(named(name)),
        });
    }
    models::SchemaResponse {
        scalar_types: scalar_types(),
        object_types,
        collections,
        functions: vec![],
        procedures,
        capabilities: Some(models::CapabilitySchemaInfo {
            query: Some(models::QueryCapabilitiesSchemaInfo {
                aggregates: Some(models::AggregateCapabilitiesSchemaInfo { count_scalar_type: "Int".into() }),
            }),
        }),
        request_arguments: None,
    }
}

pub fn capabilities() -> models::Capabilities {
    models::Capabilities {
        query: models::QueryCapabilities {
            aggregates: Some(models::AggregateCapabilities { filter_by: None, group_by: None }),
            variables: Some(models::LeafCapability {}),
            explain: Some(models::LeafCapability {}),
            nested_fields: models::NestedFieldCapabilities { filter_by: None, order_by: None, aggregates: None, nested_collections: None },
            exists: models::ExistsCapabilities { named_scopes: None, unrelated: None, nested_collections: None, nested_scalar_collections: None },
        },
        mutation: models::MutationCapabilities { transactional: None, explain: None },
        relationships: None,
        relational_query: None,
        relational_mutation: None,
    }
}

