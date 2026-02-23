use parking_lot::Mutex;
use pyo3::prelude::*;
use std::sync::Arc;
use std::time::Duration;

use connectorx::pool::PoolVariant;
use connectorx::source_router::parse_source;
use connectorx::sources::postgres::rewrite_tls_args;
use postgres::NoTls;
use r2d2_mysql::MySqlConnectionManager;
use r2d2_oracle::OracleConnectionManager;
use r2d2_postgres::PostgresConnectionManager;
use r2d2_sqlite::SqliteConnectionManager;

use crate::errors::ConnectorXPythonError;

/// Configuration for connection pool
#[derive(Debug, Clone)]
pub struct PoolConfig {
    pub max_size: u32,
    pub idle_timeout: Option<Duration>,
    pub max_lifetime: Option<Duration>,
    pub connection_timeout: Duration,
    pub test_on_check_out: bool,
}

impl Default for PoolConfig {
    fn default() -> Self {
        Self {
            max_size: 10,
            idle_timeout: Some(Duration::from_secs(300)), // 5 minutes
            max_lifetime: Some(Duration::from_secs(1800)), // 30 minutes
            connection_timeout: Duration::from_secs(30),
            test_on_check_out: true,
        }
    }
}

/// Applies the shared pool configuration to an r2d2 builder, returning the configured builder.
fn configure_builder<M: r2d2::ManageConnection>(
    builder: r2d2::Builder<M>,
    config: &PoolConfig,
) -> r2d2::Builder<M> {
    let mut builder = builder
        .max_size(config.max_size)
        .connection_timeout(config.connection_timeout);
    if let Some(timeout) = config.idle_timeout {
        builder = builder.idle_timeout(Some(timeout));
    }
    if let Some(lifetime) = config.max_lifetime {
        builder = builder.max_lifetime(Some(lifetime));
    }
    if config.test_on_check_out {
        builder = builder.test_on_check_out(true);
    }
    builder
}

/// Python-exposed connection pool class
#[pyclass]
pub struct PyConnectionPool {
    pool: Mutex<Option<PoolVariant>>,
    pub conn_str: String,
    max_size: u32,
}

#[pymethods]
impl PyConnectionPool {
    /// Create a new connection pool
    #[new]
    #[pyo3(signature = (conn, max_size=10, idle_timeout=300, max_lifetime=1800, connection_timeout=30, test_on_check_out=true))]
    pub fn new(
        conn: &str,
        max_size: u32,
        idle_timeout: Option<u64>,
        max_lifetime: Option<u64>,
        connection_timeout: u64,
        test_on_check_out: bool,
    ) -> PyResult<Self> {
        let config = PoolConfig {
            max_size,
            idle_timeout: idle_timeout.map(Duration::from_secs),
            max_lifetime: max_lifetime.map(Duration::from_secs),
            connection_timeout: Duration::from_secs(connection_timeout),
            test_on_check_out,
        };

        let source_conn = parse_source(conn, None)
            .map_err(|e| ConnectorXPythonError::from(e))?;

        let pool_variant = match source_conn.ty {
            connectorx::source_router::SourceType::MySQL => {
                use r2d2_mysql::mysql::{Opts, OptsBuilder};
                let manager = MySqlConnectionManager::new(
                    OptsBuilder::from_opts(
                        Opts::from_url(source_conn.conn.as_str())
                            .map_err(|e| pyo3::exceptions::PyValueError::new_err(format!("MySQL URL parsing error: {}", e)))?
                    )
                );
                let pool = configure_builder(r2d2::Pool::builder(), &config)
                    .build(manager)
                    .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(format!("MySQL pool creation error: {}", e)))?;
                PoolVariant::MySQL(Arc::new(pool))
            }
            connectorx::source_router::SourceType::Postgres => {
                let (pg_config, tls) = rewrite_tls_args(&source_conn.conn)
                    .map_err(|e| ConnectorXPythonError::from(e))?;

                match tls {
                    Some(tls_conn) => {
                        let manager = PostgresConnectionManager::new(pg_config, tls_conn);
                        let pool = configure_builder(r2d2::Pool::builder(), &config)
                            .build(manager)
                            .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(format!("Postgres pool creation error: {}", e)))?;
                        PoolVariant::PostgresTls(Arc::new(pool))
                    }
                    None => {
                        let manager = PostgresConnectionManager::new(pg_config, NoTls);
                        let pool = configure_builder(r2d2::Pool::builder(), &config)
                            .build(manager)
                            .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(format!("Postgres pool creation error: {}", e)))?;
                        PoolVariant::PostgresNoTls(Arc::new(pool))
                    }
                }
            }
            connectorx::source_router::SourceType::SQLite => {
                use urlencoding::decode;
                let decoded_conn = decode(source_conn.conn.path())
                    .map_err(|e| pyo3::exceptions::PyValueError::new_err(format!("SQLite path decoding error: {}", e)))?
                    .into_owned();
                let manager = SqliteConnectionManager::file(decoded_conn);

                let pool = configure_builder(r2d2::Pool::builder(), &config)
                    .build(manager)
                    .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(format!("SQLite pool creation error: {}", e)))?;
                PoolVariant::SQLite(Arc::new(pool))
            }
            connectorx::source_router::SourceType::Oracle => {
                use connectorx::sources::oracle::connect_oracle;
                use url::Url;

                let url = Url::parse(source_conn.conn.as_str())
                    .map_err(|e| pyo3::exceptions::PyValueError::new_err(format!("Oracle URL parsing error: {}", e)))?;
                let connector = connect_oracle(&url)
                    .map_err(|e| ConnectorXPythonError::from(e))?;
                let manager = OracleConnectionManager::from_connector(connector);

                let pool = configure_builder(r2d2::Pool::builder(), &config)
                    .build(manager)
                    .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(format!("Oracle pool creation error: {}", e)))?;
                PoolVariant::Oracle(Arc::new(pool))
            }
            _ => {
                return Err(pyo3::exceptions::PyValueError::new_err(
                    format!("Unsupported database type for connection pooling: {:?}", source_conn.ty)
                ));
            }
        };

        Ok(Self {
            pool: Mutex::new(Some(pool_variant)),
            conn_str: conn.to_string(),
            max_size,
        })
    }

    /// Close the pool and release all connections
    pub fn close(&self) {
        let mut pool = self.pool.lock();
        *pool = None;
    }

    /// Check if the pool is closed
    #[getter]
    pub fn is_closed(&self) -> bool {
        self.pool.lock().is_none()
    }

    /// Get the max pool size
    #[getter]
    pub fn max_size(&self) -> u32 {
        self.max_size
    }

    /// Context manager support: __enter__
    pub fn __enter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
        slf
    }

    /// Context manager support: __exit__
    pub fn __exit__(
        &self,
        _exc_type: Option<&Bound<'_, PyAny>>,
        _exc_value: Option<&Bound<'_, PyAny>>,
        _traceback: Option<&Bound<'_, PyAny>>,
    ) {
        self.close();
    }

    /// String representation
    pub fn __repr__(&self) -> String {
        let status = if self.is_closed() { "closed" } else { "open" };
        format!(
            "ConnectionPool(max_size={}, status={})",
            self.max_size, status
        )
    }
}

// Internal accessor for Rust code — returns an owned clone of the pool variant.
impl PyConnectionPool {
    pub fn get_pool_variant(&self) -> Option<PoolVariant> {
        let pool = self.pool.lock();
        pool.as_ref().cloned()
    }
}
