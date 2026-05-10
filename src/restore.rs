use std::collections::HashMap;
use std::fs;
use std::path::Path;

use anyhow::{Context, Result};
use apache_avro::{from_value, Reader, Schema};
use qdrant_client::qdrant::{
    point_id::PointIdOptions, value::Kind, vector::Vector as VectorKind, vectors::VectorsOptions,
    DenseVector, NamedVectors, PointId, PointStruct, SparseVector, UpsertPointsBuilder,
    Value as QdrantValue, Vector, Vectors,
};
use qdrant_client::Qdrant;
use serde::Deserialize;
use serde_json::Value as JsonValue;

use crate::avro_schema::POINT_SCHEMA;

#[derive(Debug, Deserialize)]
struct AvroPoint {
    id_type: String,
    id_num: Option<i64>,
    id_uuid: Option<String>,
    payload: String,
    vectors: Option<String>,
}

pub async fn run(
    url: &str,
    api_key: Option<&str>,
    input_dir: &str,
    collection_override: Option<&str>,
    batch_size: usize,
    create_if_missing: bool,
) -> Result<()> {
    let client = build_client(url, api_key)?;

    let metadata_path = Path::new(input_dir).join("metadata.json");
    let metadata: serde_json::Value = serde_json::from_str(
        &fs::read_to_string(&metadata_path)
            .with_context(|| format!("Cannot read {}", metadata_path.display()))?,
    )?;

    let collection = collection_override
        .map(|s| s.to_string())
        .or_else(|| metadata["collection_name"].as_str().map(|s| s.to_string()))
        .context(
            "No collection name: pass --collection or ensure metadata.json has 'collection_name'",
        )?;

    if create_if_missing {
        ensure_collection(&client, &collection).await?;
    }

    let avro_path = Path::new(input_dir).join("points.avro");
    let avro_file = fs::File::open(&avro_path)
        .with_context(|| format!("Cannot open {}", avro_path.display()))?;

    let schema = Schema::parse_str(POINT_SCHEMA)?;
    let reader = Reader::with_schema(&schema, avro_file)?;

    let mut batch: Vec<PointStruct> = Vec::with_capacity(batch_size);
    let mut total: u64 = 0;

    for value in reader {
        let record = value.context("Failed to read Avro record")?;
        let ap: AvroPoint = from_value(&record).context("Failed to deserialize Avro record")?;
        let point = avro_point_to_qdrant(ap)?;
        batch.push(point);

        if batch.len() >= batch_size {
            let n = batch.len() as u64;
            upsert_batch(&client, &collection, std::mem::take(&mut batch)).await?;
            total += n;
            print!("\rRestored {total} points...");
            use std::io::Write;
            std::io::stdout().flush().ok();
        }
    }

    if !batch.is_empty() {
        let n = batch.len() as u64;
        upsert_batch(&client, &collection, batch).await?;
        total += n;
    }

    println!("\nRestored {total} points into '{collection}'");
    Ok(())
}

fn build_client(url: &str, api_key: Option<&str>) -> Result<Qdrant> {
    let mut builder = Qdrant::from_url(url);
    if let Some(key) = api_key {
        builder = builder.api_key(key);
    }
    Ok(builder.build()?)
}

async fn ensure_collection(client: &Qdrant, collection: &str) -> Result<()> {
    match client.collection_info(collection).await {
        Ok(_) => {
            println!("Collection '{collection}' already exists, skipping creation.");
        }
        Err(_) => {
            anyhow::bail!(
                "Collection '{collection}' does not exist. \
                 Create it first via the Qdrant API or dashboard, \
                 then retry restore (the config is in metadata.json under 'config_debug')."
            );
        }
    }
    Ok(())
}

async fn upsert_batch(client: &Qdrant, collection: &str, points: Vec<PointStruct>) -> Result<()> {
    client
        .upsert_points(UpsertPointsBuilder::new(collection, points).wait(true))
        .await
        .context("Upsert failed")?;
    Ok(())
}

fn avro_point_to_qdrant(ap: AvroPoint) -> Result<PointStruct> {
    let id = match ap.id_type.as_str() {
        "num" => {
            let n = ap.id_num.context("id_type is 'num' but id_num is null")?;
            PointId {
                point_id_options: Some(PointIdOptions::Num(n as u64)),
            }
        }
        "uuid" => {
            let u = ap
                .id_uuid
                .context("id_type is 'uuid' but id_uuid is null")?;
            PointId {
                point_id_options: Some(PointIdOptions::Uuid(u)),
            }
        }
        other => anyhow::bail!("Unknown id_type: {other}"),
    };

    let payload_json: serde_json::Map<String, JsonValue> =
        serde_json::from_str(&ap.payload).context("Failed to parse payload JSON")?;
    let payload = json_map_to_qdrant_payload(&payload_json);

    let vectors = ap
        .vectors
        .map(|s| -> Result<Vectors> {
            let v: JsonValue =
                serde_json::from_str(&s).context("Failed to parse vectors JSON")?;
            json_to_vectors(&v)
        })
        .transpose()?;

    Ok(PointStruct {
        id: Some(id),
        payload,
        vectors,
    })
}

fn json_map_to_qdrant_payload(
    map: &serde_json::Map<String, JsonValue>,
) -> HashMap<String, QdrantValue> {
    map.iter()
        .map(|(k, v)| (k.clone(), json_to_qdrant_value(v)))
        .collect()
}

fn json_to_qdrant_value(v: &JsonValue) -> QdrantValue {
    let kind = match v {
        JsonValue::Null => Some(Kind::NullValue(0)),
        JsonValue::Bool(b) => Some(Kind::BoolValue(*b)),
        JsonValue::Number(n) => {
            if let Some(i) = n.as_i64() {
                Some(Kind::IntegerValue(i))
            } else if let Some(f) = n.as_f64() {
                Some(Kind::DoubleValue(f))
            } else {
                None
            }
        }
        JsonValue::String(s) => Some(Kind::StringValue(s.clone())),
        JsonValue::Array(arr) => {
            use qdrant_client::qdrant::ListValue;
            Some(Kind::ListValue(ListValue {
                values: arr.iter().map(json_to_qdrant_value).collect(),
            }))
        }
        JsonValue::Object(obj) => {
            use qdrant_client::qdrant::Struct;
            Some(Kind::StructValue(Struct {
                fields: obj
                    .iter()
                    .map(|(k, v)| (k.clone(), json_to_qdrant_value(v)))
                    .collect(),
            }))
        }
    };
    QdrantValue { kind }
}

/// Parse JSON produced by `backup::vectors_output_to_json` back into `Vectors`.
fn json_to_vectors(v: &JsonValue) -> Result<Vectors> {
    if let Some(inner) = v.get("vector") {
        let vec = parse_single_vector(inner)?;
        return Ok(Vectors {
            vectors_options: Some(VectorsOptions::Vector(vec)),
        });
    }

    if let Some(map) = v.get("vectors").and_then(|m| m.as_object()) {
        let mut named: HashMap<String, Vector> = HashMap::new();
        for (name, val) in map {
            named.insert(name.clone(), parse_single_vector(val)?);
        }
        return Ok(Vectors {
            vectors_options: Some(VectorsOptions::Vectors(NamedVectors { vectors: named })),
        });
    }

    anyhow::bail!("Cannot parse vectors JSON: {v}")
}

fn parse_single_vector(v: &JsonValue) -> Result<Vector> {
    if let Some(data) = v.get("dense").and_then(|a| a.as_array()) {
        let floats: Vec<f32> = data
            .iter()
            .map(|x| x.as_f64().unwrap_or(0.0) as f32)
            .collect();
        return Ok(Vector {
            vector: Some(VectorKind::Dense(DenseVector { data: floats })),
            ..Default::default()
        });
    }

    if let Some(sparse) = v.get("sparse") {
        let values: Vec<f32> = sparse
            .get("values")
            .and_then(|a| a.as_array())
            .unwrap_or(&vec![])
            .iter()
            .map(|x| x.as_f64().unwrap_or(0.0) as f32)
            .collect();
        let indices: Vec<u32> = sparse
            .get("indices")
            .and_then(|a| a.as_array())
            .unwrap_or(&vec![])
            .iter()
            .map(|x| x.as_u64().unwrap_or(0) as u32)
            .collect();
        return Ok(Vector {
            vector: Some(VectorKind::Sparse(SparseVector { values, indices })),
            ..Default::default()
        });
    }

    if let Some(vecs) = v.get("multi_dense").and_then(|a| a.as_array()) {
        use qdrant_client::qdrant::MultiDenseVector;
        let inner: Vec<DenseVector> = vecs
            .iter()
            .filter_map(|row| row.as_array())
            .map(|row| DenseVector {
                data: row
                    .iter()
                    .map(|x| x.as_f64().unwrap_or(0.0) as f32)
                    .collect(),
            })
            .collect();
        return Ok(Vector {
            vector: Some(VectorKind::MultiDense(MultiDenseVector { vectors: inner })),
            ..Default::default()
        });
    }

    anyhow::bail!("Cannot parse single vector JSON: {v}")
}
