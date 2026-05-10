/// Raw Avro schema JSON for a serialized Qdrant point.
///
/// Points are stored with:
///   - id_type: "num" | "uuid"
///   - id_num / id_uuid: the actual ID value
///   - payload: JSON string of the full payload map
///   - vectors: JSON string of the vectors (null if not fetched)
pub const POINT_SCHEMA: &str = r#"
{
  "type": "record",
  "name": "QdrantPoint",
  "fields": [
    {"name": "id_type",  "type": {"type": "enum", "name": "IdType", "symbols": ["num", "uuid"]}},
    {"name": "id_num",   "type": ["null", "long"],   "default": null},
    {"name": "id_uuid",  "type": ["null", "string"], "default": null},
    {"name": "payload",  "type": "string"},
    {"name": "vectors",  "type": ["null", "string"], "default": null}
  ]
}
"#;
