use crate::error::DatabaseError;
use sqlx::{
    Pool, Postgres,
    pool::PoolOptions,
    postgres::PgPoolOptions,
};
use std::fmt::Formatter;

#[cfg_attr(test, warn(unused))]
pub const DEFAULT_DB_PATH: &str = "postgresql://localhost:5432/postgres";

pub struct Database {
    pool: Pool<Postgres>,
}
/// Configuration builder of a redb [Database].
pub struct Builder {
    pub options: PgPoolOptions,
}
pub struct ReadOnlyDatabase {
    pool: Pool<Postgres>,
}
pub trait ReadableDatabase {
    /// Helper to access the underlying  instance.
    fn get_connection(&self) -> &Pool<Postgres>;
}

impl ReadOnlyDatabase {
    pub fn builder() -> Builder {
        Builder::new()
    }
    pub async fn create(path: &str) -> Result<Database, DatabaseError> {
        Self::builder().create(path).await
    }
    /// Opens an existing database.
    pub async fn open(path: &str, options: PgPoolOptions) -> Result<Database, DatabaseError> {
        // let db = DB::open_for_read_only(&Self::builder().options, path, error_if_log_file_exist)?;
        let db = options
            .clone()
            .connect(&path)
            .await?;
        Ok(Database::new(db))
    }
}

impl ReadableDatabase for Database {
    fn get_connection(&self) -> &Pool<Postgres> {
        &self.pool
    }
}

impl Database {
    /// Opens the specified file as a  database.
    /// * if the file does not exist, or is an empty file, a new database will be initialized in it
    /// * if the file is a valid  database, it will be opened
    /// * otherwise this function will return an error
    pub async fn create(path: &str) -> Result<Database, DatabaseError> {
        Self::builder().create(path).await
    }

    pub fn new(pool: Pool<Postgres>) -> Self {
        Self { pool }
    }

    pub fn builder() -> Builder {
        Builder::new()
    }
}

impl std::fmt::Debug for Database {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Database").finish()
    }
}

impl Builder {
    /// Construct a new [Builder] with sensible defaults.
    ///
    /// ## Defaults
    ///
    /// - `cache_size_bytes`: 1GiB
    #[allow(clippy::new_without_default)]
    pub fn new() -> Self {
        let opts = PoolOptions::default().max_connections(20);
        let result = Self { options: opts };

        result
    }

    /// Opens the specified path as a  database.
    /// * if the file does not exist, a new database will be initialized
    /// * if the file is a valid  database, it will be opened
    /// * otherwise this function will return an error
    pub async fn create(&self, path: &str) -> Result<Database, DatabaseError> {
        let db = self
            .options
            .clone()
            .connect(path)
            .await?;
        Ok(Database::new(db))
    }
}
