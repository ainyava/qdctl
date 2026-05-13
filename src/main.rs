mod avro_schema;
mod backup;
mod restore;

use anyhow::Result;
use clap::{Parser, Subcommand};

#[derive(Parser)]
#[command(
    name = "qdctl",
    about = "Backup and restore Qdrant collections to/from Avro files"
)]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    /// Scroll all points from a Qdrant collection and write to Avro + metadata.json
    Backup {
        /// Qdrant URL (e.g. http://localhost:6334)
        #[arg(long, default_value = "http://localhost:6334")]
        url: String,

        /// API key for Qdrant (optional)
        #[arg(long)]
        api_key: Option<String>,

        /// Collection name to back up (omit to back up all collections into subdirectories)
        #[arg(long)]
        collection: Option<String>,

        /// Output directory (will be created if it doesn't exist)
        #[arg(long, default_value = "backup", env = "QDCTL_OUTPUT_DIR")]
        output_dir: String,

        /// Number of points to fetch per scroll request
        #[arg(long, default_value_t = 1000)]
        batch_size: u32,
    },

    /// Restore points from Avro + metadata.json into a Qdrant collection
    Restore {
        /// Qdrant URL (e.g. http://localhost:6334)
        #[arg(long, default_value = "http://localhost:6334")]
        url: String,

        /// API key for Qdrant (optional)
        #[arg(long)]
        api_key: Option<String>,

        /// Input directory containing the Avro file and metadata.json
        #[arg(long, default_value = ".")]
        input_dir: String,

        /// Override collection name (defaults to the one stored in metadata.json)
        #[arg(long)]
        collection: Option<String>,

        /// Number of points to upsert per batch
        #[arg(long, default_value_t = 100)]
        batch_size: usize,

        /// Create the collection if it does not exist (uses metadata.json config)
        #[arg(long, default_value_t = true)]
        create_if_missing: bool,
    },
}

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();

    match cli.command {
        Command::Backup {
            url,
            api_key,
            collection,
            output_dir,
            batch_size,
        } => {
            backup::run(
                &url,
                api_key.as_deref(),
                collection.as_deref(),
                &output_dir,
                batch_size,
            )
            .await?;
        }
        Command::Restore {
            url,
            api_key,
            input_dir,
            collection,
            batch_size,
            create_if_missing,
        } => {
            restore::run(
                &url,
                api_key.as_deref(),
                &input_dir,
                collection.as_deref(),
                batch_size,
                create_if_missing,
            )
            .await?;
        }
    }

    Ok(())
}
