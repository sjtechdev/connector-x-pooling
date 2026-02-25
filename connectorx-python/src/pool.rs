use parking_lot::Mutex;
use pyo3::prelude::*;
use std::time::Duration;

use connectorx::pool::{PoolConfig, PoolVariant};
use connectorx::source_router::parse_source;

use crate::errors::ConnectorXPythonError;

/// Python-exposed connection pool class
#[pyclass]
pub struct PyConnectionPool {
    pool: Mutex<Option<PoolVariant>>,
    pub conn_str: String,
    max_size: u32,
    #[pyo3(get)]
    pub default_protocol: String,
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

        let mut rewritten_conn = conn.to_string();
        let mut default_protocol = "binary".to_string();

        let backend_split: Vec<&str> = conn.splitn(2, ':').collect();
        if backend_split.len() == 2 {
            if backend_split[0].contains("redshift") {
                rewritten_conn = format!("postgresql:{}", backend_split[1]);
                default_protocol = "cursor".to_string();
            } else if backend_split[0].contains("clickhouse") {
                rewritten_conn = format!("mysql:{}", backend_split[1]);
                default_protocol = "text".to_string();
            }
        }

        let source_conn = parse_source(&rewritten_conn, None)
            .map_err(|e| ConnectorXPythonError::from(e))?;

        let pool_variant = PoolVariant::from_source_conn(&source_conn, &config)
            .map_err(|e| ConnectorXPythonError::from(e))?
            .ok_or_else(|| {
                pyo3::exceptions::PyValueError::new_err(format!(
                    "Unsupported database type for connection pooling: {:?}",
                    source_conn.ty
                ))
            })?;

        Ok(Self {
            pool: Mutex::new(Some(pool_variant)),
            conn_str: rewritten_conn,
            max_size,
            default_protocol,
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
