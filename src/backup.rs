use std::collections::HashMap;
use std::fs;
use std::path::Path;

use anyhow::{Context, Result};
use apache_avro::{Schema, Writer};
use qdrant_client::qdrant::{
    point_id::PointIdOptions, value::Kind,
    vector_output::Vector as VectorOutputKind,
    vectors_output::VectorsOptions, ScrollPointsBuilder,
    with_payload_selector::SelectorOptions as PayloadSelectorOptions,
    with_vectors_selector::SelectorOptions as VectorSelectorOptions,
};
use qdrant_client::Qdrant;
use serde_json::{json, Value as JsonValue};

use crate::avro_schema::POINT_SCHEMA;

pub async fn run(
    url: &str,
    api_key: Option<&str>,
    collection: &str,
    output_dir: &str,
    batch_size: u32,
) -> Result<()> {
    let client = build_client(url, api_key)?;

    fs::create_dir_all(output_dir)
        .with_context(|| format!("Failed to create output directory: {output_dir}"))?;

    let info_resp = client
        .collection_info(collection)
        .await
        .with_context(|| format!("Failed to fetch collection info for '{collection}'"))?;

    let collection_info = info_resp
        .result
        .with_context(|| "Collection info result is empty")?;

    let metadata = build_metadata(collection, &collection_info);
    let metadata_path = Path::new(output_dir).join("metadata.json");
    fs::write(
        &metadata_path,
        serde_json::to_string_pretty(&metadata)?,
    )
    .with_context(|| format!("Failed to write metadata to {}", metadata_path.display()))?;
    println!("Wrote metadata → {}", metadata_path.display());

    let schema = Schema::parse_str(POINT_SCHEMA)?;
    let avro_path = Path::new(output_dir).join("points.avro");
    let avro_file = fs::File::create(&avro_path)
        .with_context(|| format!("Failed to create {}", avro_path.display()))?;
    let mut writer = Writer::new(&schema, avro_file);

    let mut offset: Option<qdrant_client::qdrant::PointId> = None;
    let mut total: u64 = 0;

    loop {
        let mut req = ScrollPointsBuilder::new(collection)
            .limit(batch_size)
            .with_payload(PayloadSelectorOptions::Enable(true))
            .with_vectors(VectorSelectorOptions::Enable(true));

        if let Some(ref off) = offset {
            req = req.offset(off.clone());
        }

        let response = client.scroll(req).await.context("Scroll request failed")?;
        let points = response.result;
        let count = points.len();

        for point in &points {
            let record = point_to_avro_record(point)?;
            writer.append_value_ref(&record)?;
        }

        total += count as u64;
        print!("\rScrolled {total} points...");
        use std::io::Write;
        std::io::stdout().flush().ok();

        offset = response.next_page_offset;
        if offset.is_none() || count == 0 {
            break;
        }
    }

    writer.flush()?;
    println!("\nWrote {total} points → {}", avro_path.display());
    Ok(())
}

fn build_client(url: &str, api_key: Option<&str>) -> Result<Qdrant> {
    let mut builder = Qdrant::from_url(url);
    if let Some(key) = api_key {
        builder = builder.api_key(key);
    }
    Ok(builder.build()?)
}

fn build_metadata(
    collection: &str,
    info: &qdrant_client::qdrant::CollectionInfo,
) -> serde_json::Value {
    let config_debug = format!("{:?}", info.config);
    json!({
        "collection_name": collection,
        "points_count": info.points_count,
        "indexed_vectors_count": info.indexed_vectors_count,
        "segments_count": info.segments_count,
        "config_debug": config_debug,
    })
}

fn point_to_avro_record(
    point: &qdrant_client::qdrant::RetrievedPoint,
) -> Result<apache_avro::types::Value> {
    let (id_type, id_num, id_uuid) = match &point.id {
        Some(pid) => match &pid.point_id_options {
            Some(PointIdOptions::Num(n)) => (
                apache_avro::types::Value::Enum(0, "num".to_string()),
                apache_avro::types::Value::Union(
                    1,
                    Box::new(apache_avro::types::Value::Long(*n as i64)),
                ),
                apache_avro::types::Value::Union(0, Box::new(apache_avro::types::Value::Null)),
            ),
            Some(PointIdOptions::Uuid(u)) => (
                apache_avro::types::Value::Enum(1, "uuid".to_string()),
                apache_avro::types::Value::Union(0, Box::new(apache_avro::types::Value::Null)),
                apache_avro::types::Value::Union(
                    1,
                    Box::new(apache_avro::types::Value::String(u.clone())),
                ),
            ),
            None => anyhow::bail!("Point has no ID options"),
        },
        None => anyhow::bail!("Point has no ID"),
    };

    let payload_json = payload_to_json(&point.payload);
    let payload_str = serde_json::to_string(&payload_json)?;

    let vectors_str = point
        .vectors
        .as_ref()
        .map(|v| -> Result<String> {
            let json = vectors_output_to_json(v);
            Ok(serde_json::to_string(&json)?)
        })
        .transpose()?;

    let vectors_avro = match vectors_str {
        Some(s) => apache_avro::types::Value::Union(
            1,
            Box::new(apache_avro::types::Value::String(s)),
        ),
        None => apache_avro::types::Value::Union(0, Box::new(apache_avro::types::Value::Null)),
    };

    Ok(apache_avro::types::Value::Record(vec![
        ("id_type".to_string(), id_type),
        ("id_num".to_string(), id_num),
        ("id_uuid".to_string(), id_uuid),
        (
            "payload".to_string(),
            apache_avro::types::Value::String(payload_str),
        ),
        ("vectors".to_string(), vectors_avro),
    ]))
}

pub fn payload_to_json(
    payload: &HashMap<String, qdrant_client::qdrant::Value>,
) -> serde_json::Map<String, JsonValue> {
    payload
        .iter()
        .map(|(k, v)| (k.clone(), qdrant_value_to_json(v)))
        .collect()
}

pub fn qdrant_value_to_json(v: &qdrant_client::qdrant::Value) -> JsonValue {
    match &v.kind {
        None => JsonValue::Null,
        Some(Kind::NullValue(_)) => JsonValue::Null,
        Some(Kind::BoolValue(b)) => JsonValue::Bool(*b),
        Some(Kind::IntegerValue(i)) => JsonValue::Number((*i).into()),
        Some(Kind::DoubleValue(d)) => serde_json::Number::from_f64(*d)
            .map(JsonValue::Number)
            .unwrap_or(JsonValue::Null),
        Some(Kind::StringValue(s)) => JsonValue::String(s.clone()),
        Some(Kind::StructValue(st)) => {
            let map: serde_json::Map<String, JsonValue> = st
                .fields
                .iter()
                .map(|(k, v)| (k.clone(), qdrant_value_to_json(v)))
                .collect();
            JsonValue::Object(map)
        }
        Some(Kind::ListValue(lv)) => {
            JsonValue::Array(lv.values.iter().map(qdrant_value_to_json).collect())
        }
    }
}

fn vectors_output_to_json(v: &qdrant_client::qdrant::VectorsOutput) -> JsonValue {
    match &v.vectors_options {
        None => JsonValue::Null,
        Some(VectorsOptions::Vector(vec_out)) => {
            let inner = match &vec_out.vector {
                Some(VectorOutputKind::Dense(d)) => json!({ "dense": d.data }),
                Some(VectorOutputKind::Sparse(s)) => {
                    json!({ "sparse": { "values": s.values, "indices": s.indices } })
                }
                Some(VectorOutputKind::MultiDense(md)) => {
                    let vecs: Vec<&Vec<f32>> = md.vectors.iter().map(|d| &d.data).collect();
                    json!({ "multi_dense": vecs })
                }
                None => JsonValue::Null,
            };
            json!({ "vector": inner })
        }
        Some(VectorsOptions::Vectors(named)) => {
            let map: serde_json::Map<String, JsonValue> = named
                .vectors
                .iter()
                .map(|(name, vec_out)| {
                    let inner = match &vec_out.vector {
                        Some(VectorOutputKind::Dense(d)) => json!({ "dense": d.data }),
                        Some(VectorOutputKind::Sparse(s)) => {
                            json!({ "sparse": { "values": s.values, "indices": s.indices } })
                        }
                        Some(VectorOutputKind::MultiDense(md)) => {
                            let vecs: Vec<&Vec<f32>> =
                                md.vectors.iter().map(|d| &d.data).collect();
                            json!({ "multi_dense": vecs })
                        }
                        None => JsonValue::Null,
                    };
                    (name.clone(), inner)
                })
                .collect();
            json!({ "vectors": map })
        }
    }
}
