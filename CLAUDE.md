# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Commands

```
cargo build                    # compile
cargo run -- backup --help     # run with args
cargo clippy                   # lint
cargo fmt                      # format
```

## Architecture

`qdctl` is a CLI tool (single binary) for backing up and restoring Qdrant collections via Avro files.

**Modules:**

- `main.rs` — CLI parsing with `clap` (subcommands: `backup`, `restore`)
- `avro_schema.rs` — Avro schema constant for the `QdrantPoint` record
- `backup.rs` — scrolls all points from Qdrant, writes `points.avro` + `metadata.json`
- `restore.rs` — reads `points.avro` + `metadata.json`, upserts points back into Qdrant

**Backup flow:** `client.scroll()` in a loop (using `next_page_offset` for pagination) → serialize each `RetrievedPoint` to an Avro record → flush to `points.avro`. Collection config is serialized as a debug string in `metadata.json` (no auto-create on restore; user must create the collection manually from that config).

**Vector encoding:** Vectors are stored as a JSON string in the Avro `vectors` field. The JSON shape is:

- Single vector: `{"vector": {"dense": [...]}}` or `{"vector": {"sparse": {...}}}` or `{"vector": {"multi_dense": [...]}}`
- Named vectors: `{"vectors": {"name": {"dense": [...]}, ...}}`

**Key types:** `qdrant-client 1.17` uses `vector::Vector` (oneof with `Dense`/`Sparse`/`MultiDense`) — not the deprecated flat `data` field on `Vector`. Use `Vector { vector: Some(VectorKind::Dense(DenseVector { data })), ..Default::default() }` when constructing for upsert.
